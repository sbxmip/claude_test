# Report Dictionary: Retail Insights

> Retail analytics report covering store banner performance, geographic effectiveness, and promotion effectiveness for a multi-banner retail chain.

**Purpose:** Enable store operations managers, marketing analysts, and category managers to evaluate banner efficiency, regional profitability with what-if scenarios, and promotional lift across product lines.

*Migrated from SAS Visual Analytics · 6 pages · 18 visuals*

---

## Data Sources

### RAND_RETAILDEMO
Retail transaction data covering sales, costs, marketing budgets, store attributes, and customer demographics across multiple banners and regions.

**Connection:** `cas-shared-default.Samples.RAND_RETAILDEMO`

| Column | Label | Kind | Type / Agg | Format |
|--------|-------|------|------------|--------|
| `brand_name1` | Brand Name 1 | Dimension | categorical | — |
| `ChannelType` | Channel Type | Dimension | categorical | — |
| `City` | City | Dimension | categorical | — |
| `City_Lat` | City Latitude | Dimension | geo (latitude) | — |
| `City_Long` | City Longitude | Dimension | geo (longitude) | — |
| `Country` | Country | Dimension | categorical | — |
| `Country_Lat` | Country Latitude | Dimension | geo (latitude) | — |
| `Country_Long` | Country Longitude | Dimension | geo (longitude) | — |
| `age_bucket` | Age Bucket | Dimension | categorical | — |
| `CustID` | Customer ID | Dimension | categorical | — |
| `loyalty_card` | Loyalty Card | Dimension | categorical | — |
| `bucket` | Bucket | Dimension | categorical | — |
| `MDY` | MDY | Dimension | categorical | — |
| `brand_name` | Brand Name | Dimension | categorical | — |
| `Class` | Class | Dimension | categorical | — |
| `Department` | Department | Dimension | categorical | — |
| `Region` | Region | Dimension | categorical | — |
| `Region` | Custom Region | Dimension | categorical | — |
| `Region_2` | Region 2 | Dimension | categorical | — |
| `Region_2_Lat` | Region 2 Latitude | Dimension | geo (latitude) | — |
| `Region_2_Long` | Region 2 Longitude | Dimension | geo (longitude) | — |
| `Region_Lat` | Region Latitude | Dimension | geo (latitude) | — |
| `Region_Long` | Region Longitude | Dimension | geo (longitude) | — |
| `State` | State | Dimension | categorical | — |
| `State` | State - Region | Dimension | categorical | — |
| `State_Lat` | State Latitude | Dimension | geo (latitude) | — |
| `State_Long` | State Longitude | Dimension | geo (longitude) | — |
| `Storechain` | Store Chain | Dimension | categorical | — |
| `Storechain1` | Store Chain 1 | Dimension | categorical | — |
| `StoreNum` | Store Number | Dimension | categorical | — |
| `Date` | Date | Dimension | time (day) | — |
| `Date` | Transaction MMYYYY | Dimension | time (month) | — |
| `Date` | Transaction Date DOW | Dimension | time (day) | — |
| `trx_dow_new` | Transaction Day of Week | Dimension | categorical | — |
| `trx_tod` | Transaction Time of Day | Dimension | categorical | — |
| `trx_hr_char` | Transaction Hour | Dimension | categorical | — |
| `Year` | Year | Dimension | time (year) | — |
| `age` | Age | Measure | average | number |
| `Margin` | Margin | Measure | sum | currency |
| `mkt_bdgt` | Marketing Budget | Measure | average | currency |
| `Sales` | Sales | Measure | sum | currency |
| `Sales` | Regular Sales Avg | Measure | average | currency |
| `Cost` | Cost | Measure | sum | currency |
| `Storeage` | Store Age | Measure | average | number |
| `sss` | Store Square Footage | Measure | average | number |
| `adjusted_sales_calc` | Adjusted Sales | Measure | sum | currency |
| `adjusted_sales_cost_calc` | Adjusted Sales Cost | Measure | sum | currency |

### PROMO_EFFECTIVENESS_X_EFFECTS_2
Promotion effectiveness data with actual, baseline, and expected revenue along with price impact metrics by product and promotion.

**Connection:** `cas-shared-default.Samples.PROMO_EFFECTIVENESS_X_EFFECTS_2`

| Column | Label | Kind | Type / Agg | Format |
|--------|-------|------|------------|--------|
| `prod_hier_sk` | Product Hierarchy Key | Dimension | categorical | — |
| `name` | Product Name | Dimension | categorical | — |
| `promotion_name` | Promotion Name | Dimension | categorical | — |
| `product_size_grouped` | Product Size | Dimension | categorical | — |
| `product_age_grouped` | Product Age | Dimension | categorical | — |
| `product_health_grouped` | Product Health | Dimension | categorical | — |
| `revenue_____actual` | Revenue Actual | Measure | sum | currency |
| `revenue_____baseline` | Revenue Baseline | Measure | sum | currency |
| `revenue_____expected` | Revenue Expected | Measure | sum | currency |
| `forecast_accuracy` | Forecast Accuracy | Measure | average | percentage |
| `revenue_____expected_change` | Revenue Expected Change | Measure | sum | currency |
| `halo___cannibal_impact` | Halo / Cannibal Impact | Measure | sum | currency |
| `own_price_impact` | Own Price Impact | Measure | sum | currency |
| `total_price_impact` | Total Price Impact | Measure | sum | currency |

---

## Metrics & Calculations

| Metric | Type | Formula / Basis | Format | Description |
|--------|------|-----------------|--------|-------------|
| **Total Sales** | simple | `sales` | currency | Sum of all sales revenue |
| **Total Cost** | simple | `cost` | currency | Sum of all costs |
| **Average Marketing Budget** | simple | `marketing_budget` | currency | Average marketing budget across transactions |
| **Total Adjusted Sales** | simple | `adjusted_sales` | currency | Sum of parameter-adjusted sales amounts |
| **Total Adjusted Sales Cost** | simple | `adjusted_sales_cost` | currency | Sum of parameter-adjusted cost amounts |
| **Average Store Square Footage** | simple | `store_square_footage` | number | Average store square footage |
| **Total Revenue Actual** | simple | `revenue_actual` | currency | Sum of actual promotion revenue |
| **Total Revenue Baseline** | simple | `revenue_baseline` | currency | Sum of baseline (no-promotion) revenue |
| **Total Revenue Expected** | simple | `revenue_expected` | currency | Sum of forecasted/expected promotion revenue |
| **Marketing Pct of Sales** | ratio | `avg_marketing_budget` / `total_sales` | percentage | Average marketing budget divided by total sales, expressing marketing spend as a percentage of sales revenue |
| **Sales per SQFT** | ratio | `total_sales` / `avg_store_square_footage` | currency | Total sales divided by average store square footage, measuring sales efficiency per unit of store space |
| **ROI** | derived | `(total_sales - total_cost) / total_cost` | percentage | Return on investment: (Total Sales - Total Cost) / Total Cost |
| **Adjusted ROI** | derived | `(total_adjusted_sales - total_adjusted_sales_cost) / total_adjusted_sales_cost` | percentage | ROI using parameter-adjusted sales and costs: (Total Adjusted Sales - Total Adjusted Sales Cost) / Total Adjusted Sales Cost. Enables what-if scenario analysis. |
| **Actual vs Forecast** | derived | `(total_revenue_actual - total_revenue_expected) / total_revenue_expected` | percentage | Percentage difference between actual and expected (forecasted) promotion revenue: (Actual - Expected) / Expected. Positive means outperformance. |
| **Actual vs Baseline** | derived | `(total_revenue_actual - total_revenue_baseline) / total_revenue_baseline` | percentage | Incremental lift from promotion above the no-promotion baseline: (Actual - Baseline) / Baseline |
| **Forecast vs Baseline** | derived | `(total_revenue_expected - total_revenue_baseline) / total_revenue_baseline` | percentage | Initially projected incremental impact of promotion: (Expected - Baseline) / Baseline |

---

## Parameters (What-if Sliders)

| Parameter | Label | Type | Default | Range | Affects |
|-----------|-------|------|---------|-------|---------|
| `sales_change` | Sales Change | decimal | 0.2 | -1.0 → 1.0 | `total_adjusted_sales`, `adjust_roi` |
| `cost_change` | Cost Change | decimal | 0.0 | -1.0 → 1.0 | `total_adjusted_sales_cost`, `adjust_roi` |

---

## Filters

| Filter | Scope | Type | Definition |
|--------|-------|------|------------|
| `filter_storechain_fast` | visual | static | in(FAST) |
| `filter_storechain_fast_with_nulls` | visual | static | in(FAST) |
| `filter_storechain_grand` | visual | static | in(GRAND) |
| `filter_storechain_grand_with_nulls` | visual | static | in(GRAND) |
| `filter_storechain_moda_with_nulls` | visual | static | in(MODA) |
| `rank_top5_sales_fast` | visual | rank | Top 5 department by total_sales |
| `rank_top5_sales_grand` | visual | rank | Top 5 department by total_sales |
| `rank_top5_sales_moda` | visual | rank | Top 5 department by total_sales |
| `filter_us_states_map` | visual | static | in(AL, AR, AZ, CA, CO, CT … (+35 more)) |
| `filter_us_states_bar` | visual | static | in(AL, AR, AZ, CA, CO, CT … (+35 more)) |
| `filter_iamz_products` | visual | static | in(Iamz Dog Active Maturity 1X17.4 LB, Iamz Dog Hlth Natl Bag 1X15.5 LB, Iamz Dog Lamb and Rice 1X17.4 LB, Iamz Dog Large Breed 1X17.4 LB, Iamz Dog Mini Chunks 1X17.4 LB, Iamz Dog Small Breed 4X7 LB … (+6 more)) |
| `filter_promo_not_missing` | visual | static | is_not_null() |

---

## Report Pages

### Page 1: Store Banner Dashboard
> Compares the performance of three store banners (FAST, GRAND, MODA) on marketing efficiency and sales by department. Answers: How effectively is each banner converting marketing spend into sales?

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Key Value - Marketing Pct of Sales (GRAND) | kpi_card | `marketing_pct_of_sales` | — | `filter_storechain_grand` |
| Key Value - Marketing Pct of Sales (MODA) | kpi_card | `marketing_pct_of_sales` | — | `filter_storechain_moda_with_nulls` |
| Key Value - Marketing Pct of Sales (FAST) | kpi_card | `marketing_pct_of_sales` | — | `filter_storechain_fast` |
| Dual Axis Bar-Line - Merchandise Hierarchy (GRAND) | combo_chart | `total_sales`, `avg_marketing_budget` | `department`, `class` | `filter_storechain_grand_with_nulls`, `rank_top5_sales_grand` |
| Dual Axis Bar-Line - Merchandise Hierarchy (MODA) | combo_chart | `total_sales`, `avg_marketing_budget` | `department`, `class` | `filter_storechain_moda_with_nulls`, `rank_top5_sales_moda` |
| Dual Axis Bar-Line - Merchandise Hierarchy (FAST) | combo_chart | `total_sales`, `avg_marketing_budget` | `department`, `class` | `filter_storechain_fast_with_nulls`, `rank_top5_sales_fast` |
| Store Banner Dashboard Text | text | — | — | — |

### Page 2: Store Banner Dashboard Information
> Hidden page with contextual help text explaining how to interpret the Store Banner Dashboard.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Store Banner Dashboard Information Text | text | — | — | — |

### Page 3: Geographic Effectiveness
> Shows sales vs adjusted sales by U.S. region and adjusted ROI by state on a choropleth map. Supports what-if analysis via a sales change slider. Answers: Which regions and states generate the highest sales and ROI?

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Sales Change Filter | filter_control | — | — | — |
| Sales Region | bar_chart | `total_sales`, `total_adjusted_sales` | `custom_region` | `filter_us_states_bar` |
| ROI Region Map | map | `adjust_roi` | `state_region` | `filter_us_states_map` |
| Geographic Effectiveness Text | text | — | — | — |

### Page 4: Geographic Effectiveness Information
> Hidden page with contextual help text explaining how to use the geographic analysis tools and what-if slider.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Geographic Effectiveness Information Text | text | — | — | — |

### Page 5: Promotion Effectiveness
> Assesses promotional success by comparing actual revenue to forecast and baseline. Answers: Are promotions generating incremental revenue? How accurate were forecasts? Which products benefit most?

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Sales vs Base Tree Map | treemap | `total_revenue_actual`, `actual_vs_baseline` | `product_name` | `filter_iamz_products` |
| Promotional Sales Lift | kpi_card | `actual_vs_baseline` | — | `filter_iamz_products` |
| Promotion Details | crosstab | `actual_vs_baseline`, `actual_vs_forecast` | `product_age`, `product_health`, `product_size` | `filter_promo_not_missing` |
| Promotion Effectiveness Text | text | — | — | — |

### Page 6: Promotion Effectiveness Information
> Hidden page with contextual help text explaining how to interpret promotion effectiveness metrics and use cross-filtering.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Promotion Effectiveness Information Text | text | — | — | — |
