# ============================================
# BELLABEAT CASE STUDY - DATA CLEANING
# File: 01_data_cleaning.R
# Dataset: Period 2 (April-May 2016)
# ============================================

library(tidyverse)
library(skimr)
library(janitor)
library(lubridate)
library(dplyr)

# ============================================
# STEP 1: LOAD DATA
# ============================================

daily_activity <- read_csv("01_Raw_Data/Daily_Level/dailyActivity_merged.csv")
sleep_day <- read_csv("01_Raw_Data/Other/sleepDay_merged.csv")
daily_calories <- read_csv("01_Raw_Data/Daily_Level/dailyCalories_merged.csv")
weight_log_info <- read_csv("01_Raw_Data/Other/weightLogInfo_merged (2).csv")
heartrate_seconds <- read_csv("01_Raw_Data/Other/heartrate_seconds_merged (2).csv")

original_users = n_distinct(daily_activity$Id)
cat("Users:", original_users, "\n\n")


# Store original counts
original_activity_rows <- nrow(daily_activity)
original_activity_users <- n_distinct(daily_activity$Id)

original_sleep_rows <- nrow(sleep_day)
original_sleep_users <- n_distinct(sleep_day$Id)

original_calories_rows <- nrow(daily_calories)
original_calories_users <- n_distinct(daily_calories$Id)

original_weight_rows <- nrow(weight_log_info)
original_weight_users <- n_distinct(weight_log_info$Id)

original_heart_rows <- nrow(heartrate_seconds)
original_heart_users <- n_distinct(heartrate_seconds$Id)


cat("Original Data:\n")
cat("Rows:", original_rows, "\n")
cat("Users:", original_users, "\n\n")

cat("Rows:", original_calories_rows, "\n")
cat("Users:", original_calories_users, "\n\n")

cat("Rows:", original_heart_rows, "\n")
cat("Users:", original_heart_users, "\n\n")

cat("Rows:", original_weight_rows, "\n")
cat("Users:", original_heart_users, "\n\n")

cat("Rows:", original_sleep_rows, "\n")
cat("Users:", original_sleep_users, "\n\n")


# ============================================
# STEP 2: INITIAL EXPLORATION
# ============================================

# Structure
glimpse(daily_activity)


# Summary statistics
skim(daily_activity)

# Check duplicates
cat("Duplicates:", sum(duplicated(daily_activity)), "\n")

# Check missing values
cat("\nMissing Values:\n")
print(colSums(is.na(daily_activity)))



# ============================================
# STEP 3: CLEAN COLUMN NAMES
# ============================================

daily_activity <- daily_activity %>%
  clean_names()

sleep_day <- sleep_day %>%
  clean_names()

daily_calories <- daily_calories %>%
  clean_names()

weight_log_info <- weight_log_info %>%
  clean_names()

heartrate_seconds <- heartrate_seconds %>%
  clean_names()


# Check new names
cat("\nCleaned Column Names:\n")
print(names(daily_activity))


# ============================================
# STEP 4: FIX DATA TYPES
# ============================================

# Convert date column
daily_activity <- daily_activity %>%
  mutate(activity_date = mdy(activity_date), id = as.character(id))

sleep_day <- sleep_day %>%
  mutate(id = as.character(id), sleep_day = mdy_hms(sleep_day))

daily_calories <- daily_calories %>%
  mutate(id = as.character(id), activity_day = mdy(activity_day))

weight_log_info <- weight_log_info %>%
  #separate(date, into = c("date", "time"), sep = " " ) %>%
  mutate(id = as.character(id),
         date = mdy_hms(date),
         log_id = as.character(log_id))

heartrate_seconds <- heartrate_seconds %>%
  mutate(id = as.character(id), time = mdy_hms(time))

# Verify conversion
# cat("\nDate range:", as.character(range(daily_activity$activity_date)), "\n")


# ============================================
# STEP 5: REMOVE DUPLICATES
# ============================================


before_dedup <- nrow(daily_activity)
daily_activity <- daily_activity %>%
  distinct()
after_dedup <- nrow(daily_activity)


before_dedup <- nrow(weight_log_info)
weight_log_info <- weight_log_info %>%
  distinct()
after_dedup <- nrow(weight_log_info)

before_dedup <- nrow(heartrate_seconds)
heartrate_seconds <- heartrate_seconds %>%
  distinct()
after_dedup <- nrow(heartrate_seconds)

before_dedup <- nrow(daily_calories)
daily_calories <- daily_calories %>%
  distinct()
after_dedup <- nrow(daily_calories)

before_dedup <- nrow(sleep_day)
sleep_day <- sleep_day %>%
  distinct()
after_dedup <- nrow(sleep_day)


# ============================================
# STEP 6: HANDLE MISSING VALUES
# ============================================

# Check NA pattern
na_summary <- daily_activity %>%
  summarise(across(everything(), ~ sum(is.na(.))))

print(na_summary)

# Remove rows with NAs (if any)
daily_activity <- daily_activity %>%
  drop_na()


# Check NA pattern
na_summary <- sleep_day %>%
  summarise(across(everything(), ~ sum(is.na(.))))

print(na_summary)


# Check NA pattern
na_summary <- daily_calories %>%
  summarise(across(everything(), ~ sum(is.na(.))))

print(na_summary)


# Check NA pattern
na_summary <- heartrate_seconds %>%
  summarise(across(everything(), ~ sum(is.na(.))))

print(na_summary)

# Check NA pattern
na_summary <- weight_log_info %>%
  summarise(across(everything(), ~ sum(is.na(.))))

print(na_summary)

# Remove rows with NAs (if any)
weight_log_info <- weight_log_info %>%
  select(-fat)



# ============================================
# STEP 7: HANDLE INVALID VALUES & OUTLIERS
# ============================================

# Check for negative values
negative_check <- daily_activity %>%
  filter(total_steps < 0 | calories < 0)

cat("\nNegative values found:", nrow(negative_check), "\n")

# Remove completely empty days (all zeros)
daily_activity <- daily_activity %>%
  filter(!(
    total_steps == 0 &
      total_distance == 0 &
      calories == 0 &
      sedentary_minutes == 1440
  ))  # Full day sedentary

# Check outliers for steps
cat("\nSteps distribution:\n")
summary(daily_activity$total_steps)


# Only remove if > 40,000 steps (unrealistic)
daily_activity <- daily_activity %>%
  filter(total_steps <= 40000)



# Check for negative values
negative_check <- sleep_day %>%
  filter(total_sleep_records < 0 |
           total_minutes_asleep < 0 | total_time_in_bed < 0)
cat("\nNegative values found:", nrow(negative_check), "\n")

# Remove completely empty days (all zeros)
sleep_day <- sleep_day %>%
  filter(!(
    total_sleep_records == 0 &
      total_minutes_asleep == 0 &
      total_time_in_bed == 0
  ))



# Check for negative values
negative_check <- heartrate_seconds %>%
  filter(value < 0)
cat("\nNegative values found:", nrow(negative_check), "\n")

# Remove completely empty days (all zeros)
heartrate_seconds <- heartrate_seconds %>%
  filter(!(value == 0))



# Check for negative values
negative_check <- daily_calories %>%
  filter(calories < 0)
cat("\nNegative values found:", nrow(negative_check), "\n")

# Remove completely empty days (all zeros)
daily_calories <- daily_calories %>%
  filter(!(calories == 0))


# Check for negative values
negative_check <- weight_log_info %>%
  filter(weight_kg < 0 | weight_pounds < 0 | bmi < 0)
cat("\nNegative values found:", nrow(negative_check), "\n")

# Remove completely empty days (all zeros)
weight_log_info <- weight_log_info %>%
  filter(!(weight_kg == 0 &
             weight_pounds == 0 &
             bmi == 0))



# ============================================
# STEP 8: CREATE NEW VARIABLES
# ============================================

daily_activity <- daily_activity %>%
  mutate(
    # Day of week
    day_of_week = wday(activity_date, label = TRUE, abbr = FALSE),
    
    # Weekend vs Weekday
    day_type = ifelse(day_of_week %in% c("Saturday", "Sunday"), "Weekend", "Weekday"),
    
    day_type = as.factor(day_type),
    
    # Activity level categories
    activity_level = case_when(
      total_steps < 5000 ~ "Sedentary",
      total_steps < 7500 ~ "Lightly Active",
      total_steps < 10000 ~ "Fairly Active",
      total_steps < 12500 ~ "Very Active",
      TRUE ~ "Highly Active"
    ),
    activity_level = as.factor(activity_level),
    
    # Active minutes total
    total_active_minutes = very_active_minutes +
      fairly_active_minutes +
      lightly_active_minutes,
    
    # Active vs sedentary ratio
    active_ratio = total_active_minutes /
      (total_active_minutes + sedentary_minutes),
    
    # Week number
    week_num = week(activity_date),
    # Week number as integer
    week_num = as.integer(week_num)
  )



# ============================================
# STEP 9: FINAL VALIDATION
# ============================================
# DALIY_ACTIVITY
cat("\n=== DALIY_ACTIVITY CLEANING SUMMARY ===\n")
cat("Original rows:", original_activity_rows, "\n")
cat("Final rows:", nrow(daily_activity), "\n")
cat("Rows removed:",
    original_activity_rows - nrow(daily_activity),
    "\n")
cat("Removal %:", round((
  original_activity_rows - nrow(daily_activity)
) / original_rows * 100, 2), "%\n\n")

cat("Original users:", original_activity_users, "\n")
cat("Final users:", n_distinct(daily_activity$id), "\n\n")

# Final data check
cat("Final data summary:\n")
skim(daily_activity)

# Check for remaining issues
cat("\nFinal NA check:", sum(is.na(daily_activity)), "\n")
cat("Final duplicates:", sum(duplicated(daily_activity)), "\n")


# SLEEP_DAY
cat("\n=== SLEEP_DAY CLEANING SUMMARY ===\n")
cat("Original rows:", original_sleep_rows, "\n")
cat("Final rows:", nrow(sleep_day), "\n")
cat("Rows removed:", original_sleep_rows - nrow(sleep_day), "\n")
cat("Removal %:", round((original_sleep_rows - nrow(sleep_day)) / original_sleep_rows * 100, 2), "%\n\n")

cat("Original users:", original_sleep_users, "\n")
cat("Final users:", n_distinct(sleep_day$id), "\n\n")

# Final data check
cat("Final data summary:\n")
skim(sleep_day)

# Check for remaining issues
cat("\nFinal NA check:", sum(is.na(sleep_day)), "\n")
cat("Final duplicates:", sum(duplicated(sleep_day)), "\n")


# HEARTRATE_SECONDS
cat("\n=== HEARTRATE_SECONDS CLEANING SUMMARY ===\n")
cat("Original rows:", original_heart_rows, "\n")
cat("Final rows:", nrow(heartrate_seconds), "\n")
cat("Rows removed:",
    original_heart_rows - nrow(heartrate_seconds),
    "\n")
cat("Removal %:", round((
  original_heart_rows - nrow(heartrate_seconds)
) / original_heart_rows * 100, 2), "%\n\n")

cat("Original users:", original_heart_users, "\n")
cat("Final users:", n_distinct(heartrate_seconds$id), "\n\n")

# Final data check
cat("Final data summary:\n")
skim(heartrate_seconds)

# Check for remaining issues
cat("\nFinal NA check:", sum(is.na(heartrate_seconds)), "\n")
cat("Final duplicates:", sum(duplicated(heartrate_seconds)), "\n")


# DAILY_CALORIES
cat("\n=== DAILY_CALORIES CLEANING SUMMARY ===\n")
cat("Original rows:", original_calories_rows, "\n")
cat("Final rows:", nrow(daily_calories), "\n")
cat("Rows removed:",
    original_calories_rows - nrow(daily_calories),
    "\n")
cat("Removal %:", round((original_calories_rows - nrow(daily_calories)) /
                          original_calories_rows * 100,
                        2
), "%\n\n")

cat("Original users:", original_calories_users, "\n")
cat("Final users:", n_distinct(daily_calories$id), "\n\n")

# Final data check
cat("Final data summary:\n")
skim(daily_calories)

# Check for remaining issues
cat("\nFinal NA check:", sum(is.na(daily_calories)), "\n")
cat("Final duplicates:", sum(duplicated(daily_calories)), "\n")



# WEIGHT_LOG_INFO
cat("\n=== WEIGHT_LOG_INFO CLEANING SUMMARY ===\n")
cat("Original rows:", original_weight_rows, "\n")
cat("Final rows:", nrow(weight_log_info), "\n")
cat("Rows removed:",
    original_weight_rows - nrow(weight_log_info),
    "\n")
cat("Removal %:", round((
  original_weight_rows - nrow(weight_log_info)
) / original_weight_rows * 100, 2), "%\n\n")

cat("Original users:", original_weight_users, "\n")
cat("Final users:", n_distinct(weight_log_info$id), "\n\n")

# Final data check
cat("Final data summary:\n")
skim(weight_log_info)

# Check for remaining issues
cat("\nFinal NA check:", sum(is.na(weight_log_info)), "\n")
cat("Final duplicates:", sum(duplicated(weight_log_info)), "\n")

# ============================================
# STEP 10: SAVE CLEANED DATA
# ============================================

write_csv(daily_activity,
          "02_Cleaned_Data/daily_activity_cleaned.csv")
write_csv(sleep_day, "02_Cleaned_Data/sleep_day_cleaned.csv")
write_csv(heartrate_seconds,
          "02_Cleaned_Data/heartrate_seconds_cleaned.csv")
write_csv(daily_calories,
          "02_Cleaned_Data/daily_calories_cleaned.csv")
write_csv(weight_log_info,
          "02_Cleaned_Data/weight_log_info_cleaned.csv")
