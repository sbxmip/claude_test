# Report Dictionary: Retail Insights

> Retail operations dashboard covering store banner performance, geographic sales analysis, and promotional effectiveness.

**Purpose:** Enable retail executives and marketing managers to compare store banner performance, evaluate geographic ROI with what-if scenarios, and assess promotional effectiveness against forecast and baseline revenue.

*Migrated from SAS Visual Analytics · 6 pages · 18 visuals*

---

## Data Sources

### RAND_RETAILDEMO
Retail demo transaction data with store, product, customer, and geographic information.

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
| `Region_2` | Region 2 | Dimension | categorical | — |
| `Region_2_Lat` | Region 2 Latitude | Dimension | geo (latitude) | — |
| `Region_2_Long` | Region 2 Longitude | Dimension | geo (longitude) | — |
| `Region_Lat` | Region Latitude | Dimension | geo (latitude) | — |
| `Region_Long` | Region Longitude | Dimension | geo (longitude) | — |
| `State` | State | Dimension | categorical | — |
| `State_Lat` | State Latitude | Dimension | geo (latitude) | — |
| `State_Long` | State Longitude | Dimension | geo (longitude) | — |
| `Storechain` | Store Chain | Dimension | categorical | — |
| `Storechain1` | Store Chain 1 | Dimension | categorical | — |
| `StoreNum` | Store Number | Dimension | categorical | — |
| `Date` | Transaction Date | Dimension | time (day) | — |
| `Date` | Transaction MMYYYY | Dimension | time (month) | — |
| `Date` | Transaction Date DOW | Dimension | time (day) | — |
| `trx_dow_new` | Transaction Day of Week | Dimension | categorical | — |
| `trx_tod` | Transaction Time of Day | Dimension | categorical | — |
| `Year` | Year | Dimension | time (year) | — |
| `trx_hr_char` | Transaction Hour | Dimension | categorical | — |
| `Region` | Custom Region | Dimension | categorical | — |
| `State` | State - Region | Dimension | categorical | — |
| `age` | Age | Measure | average | number |
| `Margin` | Margin | Measure | sum | currency |
| `mkt_bdgt` | Marketing Budget | Measure | sum | currency |
| `mkt_bdgt` | Marketing Budget (Average) | Measure | average | currency |
| `Sales` | Sales | Measure | sum | currency |
| `Sales` | Regular Sales Avg | Measure | average | currency |
| `Cost` | Cost | Measure | sum | currency |
| `Storeage` | Store Age | Measure | average | number |
| `sss` | Store Square Footage | Measure | average | number |
| `adjusted_sales_calculated` | Adjusted Sales | Measure | sum | currency |
| `adjusted_sales_cost_calculated` | Adjusted Sales Cost | Measure | sum | currency |

### PROMO_EFFECTIVENESS_X_EFFECTS_2
Promotion effectiveness data with actual, baseline, and expected revenue along with price impact metrics.

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
| **Total Sales** | simple | `sales` | currency | Sum of all sales transactions |
| **Total Cost** | simple | `cost` | currency | Sum of all transaction costs |
| **Total Marketing Budget** | simple | `marketing_budget` | currency | Sum of marketing budget |
| **Average Marketing Budget** | simple | `marketing_budget_avg` | currency | Average marketing budget per record within grouping |
| **Regular Sales Avg** | simple | `sales_avg` | currency | Average sales per transaction |
| **Total Margin** | simple | `margin` | currency | Sum of profit margin |
| **Total Adjusted Sales** | simple | `adjusted_sales` | currency | Sum of adjusted sales (what-if scenario driven by sales_change parameter) |
| **Total Adjusted Sales Cost** | simple | `adjusted_sales_cost` | currency | Sum of adjusted cost (what-if scenario driven by cost_change parameter) |
| **Average Store Square Footage** | simple | `store_square_footage` | number | Average store square footage within grouping |
| **Marketing Pct of Sales** | ratio | `avg_marketing_budget` / `total_sales` | percentage | Average marketing budget divided by total sales. Shows what percentage of sales revenue is allocated to marketing. |
| **Sales per SQFT** | ratio | `total_sales` / `avg_store_square_footage` | currency | Total sales divided by average store square footage. Measures sales productivity per unit of floor space. |
| **ROI** | derived | `(total_sales - total_cost) / total_cost` | percentage | Return on investment: (Total Sales - Total Cost) / Total Cost |
| **Adjusted Sales** | derived | `IF region_2 IN ('US_MW','US_CS','US_AT','LATA','EU','ASIA') THEN sum(sales) * (0.85 + sales_change) ELSE sum(sales) * (1 + sales_change)` | currency | What-if adjusted sales. For regions US_MW, US_CS, US_AT, LATA, EU, ASIA: Sales * (0.85 + sales_change). For other regions: Sales * (1 + sales_change). Row-level calculation aggregated as sum. |
| **Adjusted Sales Cost** | derived | `sum(cost) * (1 + cost_change)` | currency | What-if adjusted cost: Cost * (1 + cost_change). Row-level calculation aggregated as sum. |
| **Adjust ROI** | derived | `(total_adjusted_sales - total_adjusted_sales_cost) / total_adjusted_sales_cost` | percentage | Adjusted ROI using what-if parameters: (Total Adjusted Sales - Total Adjusted Sales Cost) / Total Adjusted Sales Cost |
| **Total Revenue Actual** | simple | `revenue_actual` | currency | Sum of actual promotional revenue |
| **Total Revenue Baseline** | simple | `revenue_baseline` | currency | Sum of baseline revenue (no promotion) |
| **Total Revenue Expected** | simple | `revenue_expected` | currency | Sum of forecasted/expected promotional revenue |
| **Actual vs Forecast** | derived | `(total_revenue_actual - total_revenue_expected) / total_revenue_expected` | percentage | Percentage difference between actual and forecasted revenue: (Actual - Expected) / Expected. Positive means actual exceeded forecast. |
| **Actual vs Baseline** | derived | `(total_revenue_actual - total_revenue_baseline) / total_revenue_baseline` | percentage | Incremental lift of promotion: (Actual - Baseline) / Baseline. Measures incremental revenue over no-promotion baseline. |
| **Forecast vs Baseline** | derived | `(total_revenue_expected - total_revenue_baseline) / total_revenue_baseline` | percentage | Projected incremental impact: (Expected - Baseline) / Baseline. Measures forecasted revenue lift over baseline. |
| **Total Revenue Expected Change** | simple | `revenue_expected_change` | currency | Sum of expected revenue change from promotions |
| **Total Halo / Cannibal Impact** | simple | `halo_cannibal_impact` | currency | Sum of halo and cannibalization revenue impact |
| **Total Own Price Impact** | simple | `own_price_impact` | currency | Sum of own-price impact on revenue |
| **Total Price Impact** | simple | `total_price_impact` | currency | Sum of total price impact on revenue |
| **Average Forecast Accuracy** | simple | `forecast_accuracy` | percentage | Average forecast accuracy across promotions |
| **Average Customer Age** | simple | `age` | number | Average customer age |
| **Average Store Age** | simple | `store_age` | number | Average age of stores |

---

## Parameters (What-if Sliders)

| Parameter | Label | Type | Default | Range | Affects |
|-----------|-------|------|---------|-------|---------|
| `sales_change` | Sales Change | decimal | 0 | -0.5 → 0.5 | `adjusted_sales_metric`, `total_adjusted_sales`, `adjust_roi` |
| `cost_change` | Cost Change | decimal | 0 | -0.5 → 0.5 | `adjusted_cost_metric`, `total_adjusted_sales_cost`, `adjust_roi` |

---

## Filters

| Filter | Scope | Type | Definition |
|--------|-------|------|------------|
| `filter_storechain_fast` | visual | static | in(FAST) |
| `filter_storechain_fast_with_nulls` | visual | static | in(FAST) |
| `filter_storechain_grand` | visual | static | in(GRAND) |
| `filter_storechain_grand_with_nulls` | visual | static | in(GRAND) |
| `filter_storechain_moda_with_nulls` | visual | static | in(MODA) |
| `filter_us_states` | visual | static | in(AL, AR, AZ, CA, CO, CT … (+35 more)) |
| `filter_iamz_products` | visual | static | in(Iamz Dog Active Maturity 1X17.4 LB, Iamz Dog Hlth Natl Bag 1X15.5 LB, Iamz Dog Lamb and Rice 1X17.4 LB, Iamz Dog Large Breed 1X17.4 LB, Iamz Dog Mini Chunks 1X17.4 LB, Iamz Dog Small Breed 4X7 LB … (+6 more)) |
| `filter_promo_not_missing` | visual | static | is_not_null() |
| `rank_top5_fast_dept` | visual | rank | Top 5 department by total_sales |
| `rank_top5_grand_dept` | visual | rank | Top 5 department by total_sales |
| `rank_top5_moda_dept` | visual | rank | Top 5 department by total_sales |

---

## Report Pages

### Page 1: Store Banner Dashboard
> Compares three store banners (FAST, GRAND, MODA) on Marketing % of Sales, Sales by department, and Marketing Budget. Helps identify which banners are converting marketing spend into sales most efficiently.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Marketing Pct of Sales - FAST | kpi_card | `marketing_pct_of_sales` | — | `filter_storechain_fast` |
| Marketing Pct of Sales - GRAND | kpi_card | `marketing_pct_of_sales` | — | `filter_storechain_grand` |
| Marketing Pct of Sales - MODA | kpi_card | `marketing_pct_of_sales` | — | `filter_storechain_moda_with_nulls` |
| Dual Axis Bar-Line - FAST | combo_chart | `total_sales`, `total_marketing_budget` | `department` | `filter_storechain_fast_with_nulls`, `rank_top5_fast_dept` |
| Dual Axis Bar-Line - GRAND | combo_chart | `total_sales`, `total_marketing_budget` | `department` | `filter_storechain_grand_with_nulls`, `rank_top5_grand_dept` |
| Dual Axis Bar-Line - MODA | combo_chart | `total_sales`, `total_marketing_budget` | `department` | `filter_storechain_moda_with_nulls`, `rank_top5_moda_dept` |
| Store Banner Dashboard Text | text | — | — | — |

### Page 2: Geographic Effectiveness
> Analyzes sales and ROI by region and state. Includes what-if sliders for sales and cost adjustments to evaluate regions for new store openings or improvements.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| ROI Region Map | map | `adjust_roi` | `state`, `state_lat`, `state_long` | `filter_us_states` |
| Sales Region | bar_chart | `total_sales`, `total_adjusted_sales` | `region` | `filter_us_states` |
| Gauge 1 | kpi_card | `adjust_roi` | — | — |
| Sales Change Filter | filter_control | — | — | — |
| Geographic Effectiveness Text | text | — | — | — |

### Page 3: Promotion Effectiveness
> Evaluates promotional performance by comparing actual revenue to forecast and baseline revenue. Helps marketers understand incremental lift and forecast accuracy for specific product promotions.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Sales vs Base Tree Map | treemap | `actual_vs_baseline` | `product_name` | `filter_iamz_products` |
| Promotion Details | crosstab | `total_revenue_actual`, `total_revenue_baseline`, `total_revenue_expected`, `total_revenue_expected_change`, `total_halo_cannibal_impact`, `total_own_price_impact`, `total_total_price_impact`, `actual_vs_forecast`, `actual_vs_baseline`, `forecast_vs_baseline` | `product_size`, `product_age`, `product_health` | `filter_promo_not_missing` |
| Promotion Effectiveness Text | text | — | — | — |

### Page 4: Store Banner Dashboard Information
> Hidden help page explaining how to interpret the Store Banner Dashboard. Accessed via 'More information' link.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Store Banner Dashboard Information Text | text | — | — | — |

### Page 5: Geographic Effectiveness Information
> Hidden help page explaining how to use the Geographic Effectiveness page, including slider interactions and geo map tooltips.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Geographic Effectiveness Information Text | text | — | — | — |

### Page 6: Promotion Effectiveness Information
> Hidden help page explaining how to use the Promotion Effectiveness page, including product selection and promotional sales lift interpretation.

| Visual | Type | Metrics | Dimensions | Filters |
|--------|------|---------|------------|---------|
| Promotion Effectiveness Information Text | text | — | — | — |
