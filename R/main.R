library(arrow)
library(duckdb)
library(DBI)
library(data.table)
library(logger)
library(gargle)
library(googleCloudStorageR)

METHODOLOGY_VERSION <- "2.0.0"
BUCKET <- "sondre_brreg_data"
CHANGELOG_PREFIX <- "aksjeeierbok/cdc/changelog"
OUTPUT_PREFIX <- "aksjeeierbok/state/by_year"
START_YEAR <- 2004

list_changelog_years <- function() {
  objs <- gcs_list_objects(bucket = BUCKET, prefix = paste0(CHANGELOG_PREFIX, "/"))
  if (nrow(objs) == 0) return(integer(0))
  m <- regmatches(objs$name, regexpr("[0-9]{4}\\.parquet$", objs$name))
  years <- as.integer(substr(m, 1, 4))
  sort(unique(years[!is.na(years)]))
}

download_changelog_year <- function(year) {
  local <- sprintf("/tmp/changelog_%d.parquet", year)
  if (file.exists(local)) return(local)
  gcs_get_object(
    object_name = sprintf("%s/%d.parquet", CHANGELOG_PREFIX, year),
    bucket = BUCKET,
    saveToDisk = local,
    overwrite = TRUE
  )
  local
}

compute_year_state <- function(con, through_year) {
  dbExecute(con, sprintf("
    CREATE OR REPLACE TEMPORARY VIEW state_%d AS
    WITH latest_per_key AS (
      SELECT
        orgnr,
        document_id,
        event_type,
        CAST(valid_time AS TIMESTAMP) AS valid_time_ts,
        CAST(details_json AS VARCHAR) AS details_str,
        row_number() OVER (
          PARTITION BY document_id
          ORDER BY CAST(valid_time AS TIMESTAMP) DESC, event_type
        ) AS rn
      FROM changelog
      WHERE document_id IS NOT NULL
    )
    SELECT
      orgnr,
      document_id,
      json_extract_string(details_str, '$.shareholder_id')   AS shareholder_id,
      json_extract_string(details_str, '$.shareholder_type') AS shareholder_type,
      json_extract_string(details_str, '$.aksjeklasse')      AS aksjeklasse,
      json_extract_string(details_str, '$.landkode')         AS landkode,
      json_extract_string(details_str, '$.margin')           AS margin,
      CAST(json_extract(details_str, '$.curr_antall_aksjer') AS DOUBLE) AS antall_aksjer,
      CAST(json_extract(details_str, '$.curr_ownership_pct') AS DOUBLE) AS ownership_pct,
      %d AS as_of_year,
      CAST('%d-12-31' AS DATE) AS valid_time,
      event_type AS last_event_type
    FROM latest_per_key
    WHERE rn = 1 AND event_type <> 'disappeared'
  ", through_year, through_year, through_year))

  local <- sprintf("/tmp/state_%d.parquet", through_year)
  dbExecute(con, sprintf(
    "COPY (SELECT * FROM state_%d) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
    through_year, local
  ))
  n_rows <- dbGetQuery(con, sprintf("SELECT COUNT(*) n FROM state_%d", through_year))$n
  dbExecute(con, sprintf("DROP VIEW state_%d", through_year))
  list(path = local, rows = n_rows)
}

upload_to_gcs <- function(local_path, year) {
  gcs_upload(
    file = local_path,
    bucket = BUCKET,
    name = sprintf("%s/%d.parquet", OUTPUT_PREFIX, year),
    predefinedAcl = "bucketLevel"
  )
}

main <- function() {
  log_info("aksjeeierbok-state-materializer v{METHODOLOGY_VERSION} starting")
  t0 <- Sys.time()

  key_path <- Sys.getenv("GOOGLE_APPLICATION_CREDENTIALS", unset = "")
  if (nzchar(key_path) && file.exists(key_path)) {
    log_info("auth via service account key file {key_path}")
    gcs_auth(json_file = key_path)
  } else {
    log_info("auth via GCE metadata (default application credentials)")
    token <- gargle::credentials_gce(
      scopes = "https://www.googleapis.com/auth/devstorage.read_write"
    )
    if (is.null(token)) stop("gargle::credentials_gce returned NULL — not on a GCE instance?")
    gcs_auth(token = token)
  }
  gcs_global_bucket(BUCKET)

  all_years <- list_changelog_years()
  all_years <- all_years[all_years >= START_YEAR]
  log_info("processing years {min(all_years)}..{max(all_years)} ({length(all_years)} files)")

  con <- dbConnect(duckdb::duckdb(), ":memory:")
  dbExecute(con, "INSTALL json")
  dbExecute(con, "LOAD json")
  dbExecute(con, "SET threads=4")
  dbExecute(con, "SET memory_limit='12GB'")

  yr_files <- c()
  summary <- list()

  for (yr in all_years) {
    log_info("downloading changelog/{yr}.parquet")
    local <- download_changelog_year(yr)
    yr_files <- c(yr_files, local)

    log_info("registering {length(yr_files)} changelog files as DuckDB arrow view")
    ds <- arrow::open_dataset(yr_files, format = "parquet")
    duckdb::duckdb_register_arrow(con, "changelog", ds)

    t1 <- Sys.time()
    res <- compute_year_state(con, yr)
    elapsed <- round(difftime(Sys.time(), t1, units = "secs"), 1)
    log_info("  year {yr}: {res$rows} rows in {elapsed}s")

    log_info("  uploading gs://{BUCKET}/{OUTPUT_PREFIX}/{yr}.parquet")
    upload_to_gcs(res$path, yr)
    file.remove(res$path)

    duckdb::duckdb_unregister_arrow(con, "changelog")
    rm(ds); gc(verbose = FALSE)

    summary[[as.character(yr)]] <- list(year = yr, rows = res$rows)
  }

  dbDisconnect(con, shutdown = TRUE)
  total_rows <- sum(sapply(summary, `[[`, "rows"))
  total_elapsed <- round(difftime(Sys.time(), t0, units = "mins"), 1)
  log_info("complete: {length(summary)} years materialized, {total_rows} total rows, {total_elapsed}min")
}

main()
