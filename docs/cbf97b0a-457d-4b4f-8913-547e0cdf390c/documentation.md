# Report: Retail Insights

**Report ID:** `cbf97b0a-457d-4b4f-8913-547e0cdf390c`  
**Last modified:** 2018-06-21  
**Created by:** sas.SASVisualAnalytics  
**Creation date:** 2018-05-01  

---

## 1. Executive Summary

**Retail Insights** is a retail analytics report that serves store operations managers, marketing analysts, and category managers. It covers the performance of a multi-banner retail chain operating under three store banners — **FAST**, **GRAND**, and **MODA** — across multiple U.S. regions and product departments.

The report answers three key business questions:

1. **Banner Efficiency** — How effectively is each store banner converting its marketing spend into sales? Which departments within each banner are over- or under-performing relative to their marketing budgets?
2. **Geographic Performance** — Which U.S. regions and states generate the highest sales and ROI, and how would hypothetical changes in sales or cost assumptions affect regional profitability?
3. **Promotion Effectiveness** — Are promotions generating incremental revenue above baseline? How accurate were the revenue forecasts, and which individual products are benefiting most (or least) from promotional activity?

---

## 2. Report Structure

The report contains **6 sections** — 3 visible pages and 3 hidden information pages.

| # | Section Name | Type | Purpose |
|---|---|---|---|
| 0 | **Store Banner Dashboard** | Visible | Compares FAST, GRAND, and MODA banners on Marketing % of Sales and Sales vs. Marketing Budget by department. Three KPI cards + three dual-axis bar-line charts. |
| 1 | Store Banner Dashboard Information | Hidden | Contextual help text explaining how to interpret the banner dashboard. |
| 2 | **Geographic Effectiveness** | Visible | Shows Sales vs. Adjusted Sales by U.S. region (bar chart), Adjusted ROI by state (Esri choropleth map), and a slider prompt for what-if sales change analysis. |
| 3 | Geographic Effectiveness Information | Hidden | Contextual help text for the geographic analysis page. |
| 4 | **Promotion Effectiveness** | Visible | Tree map of Actual vs. Baseline by product, a gauge showing overall Promotional Sales Lift, a crosstab of product attributes with Actual vs. Baseline and Actual vs. Forecast, and a bar legend for Revenue Actual + Actual vs. Baseline. |
| 5 | Promotion Effectiveness Information | Hidden | Contextual help text for the promotion analysis page. |

---

## 3. Data Model

### 3.1 RAND_RETAILDEMO

- **CAS Path:** `cas-shared-default` → `Samples.RAND_RETAILDEMO`
- **Type:** Relational

| Column | Apparent Type | Role |
|---|---|---|
| `brand_name1` | String | Dimension — brand variant |
| `ChannelType` | String | Dimension — sales channel |
| `City` | String | Dimension — city name |
| `City_Lat` / `City_Long` | Numeric | Geo-coordinate (city) |
| `Country` | String | Dimension — country |
| `Country_Lat` / `Country_Long` | Numeric | Geo-coordinate (country) |
| `age` | Numeric | Measure — customer age |
| `age_bucket` | String | Dimension — age band |
| `CustID` | String/Numeric | Dimension — customer identifier |
| `loyalty_card` | String | Dimension — loyalty card flag |
| `bucket` | String | Dimension — generic bucket |
| `Margin` | Numeric | Measure — margin amount |
| `mkt_bdgt` | Numeric | Measure — marketing budget |
| `MDY` | Date/String | Dimension — month-day-year |
| `brand_name` | String | Dimension — brand name |
| `Class` | String | Dimension — product class |
| `Department` | String | Dimension — product department |
| `Region` | String | Dimension — region (Custom Region derived) |
| `Region_2` | String | Dimension — sub-region |
| `Region_2_Lat` / `Region_2_Long` | Numeric | Geo-coordinate (sub-region) |
| `Region_Lat` / `Region_Long` | Numeric | Geo-coordinate (region) |
| `Sales` | Numeric | Measure — sales amount |
| `Cost` | Numeric | Measure — cost amount |
| `State` | String | Dimension — U.S. state (also used as "State - Region") |
| `State_Lat` / `State_Long` | Numeric | Geo-coordinate (state) |
| `Storeage` | Numeric | Dimension/Measure — store age |
| `Storechain` | String | Dimension — store chain name |
| `Storechain1` | String | Dimension — store chain variant |
| `StoreNum` | String/Numeric | Dimension — store number |
| `sss` | Numeric | Measure — store square footage |
| `Date` | Date | Dimension — transaction date (also formatted as `Transaction MMYYYY` and `Transaction Date DOW`) |
| `trx_dow_new` | String | Dimension — transaction day of week |
| `trx_tod` | String | Dimension — transaction time of day |
| `trx_hr_char` | String | Dimension — transaction hour (character) |
| `Year` | Numeric/String | Dimension — year |

**Derived DataItems:**
- `Transaction MMYYYY` (bi234) — Date formatted as month-year
- `State - Region` (bi362) — State with region context
- `Regular Sales Avg` (bi382) — Sales with average aggregation
- `Transaction Date DOW` (bi1488) — Date formatted as day of week
- `Custom Region` (bi6322) — Region with custom grouping

**Hierarchies:**
| Hierarchy | ID | Levels (coarse → fine) |
|---|---|---|
| Merchandise Hierarchy | bi157 | Department (xref: Department) → Class (xref: Class) |
| Hierarchy 1 | bi6456 | Custom Region (xref: Region) → State (xref: State) |
| Hierarchy 2 | bi6505 | Region_2 (xref: Region_2) → State (xref: State) |

### 3.2 PROMO_EFFECTIVENESS_X_EFFECTS_2

- **CAS Path:** `cas-shared-default` → `Samples.PROMO_EFFECTIVENESS_X_EFFECTS_2`
- **Type:** Relational

| Column | Apparent Type | Role |
|---|---|---|
| `revenue_____actual` | Numeric | Measure — actual revenue |
| `revenue_____baseline` | Numeric | Measure — baseline revenue (no-promotion scenario) |
| `revenue_____expected` | Numeric | Measure — forecasted/expected revenue |
| `forecast_accuracy` | Numeric | Measure — forecast accuracy metric |
| `revenue_____expected_change` | Numeric | Measure — expected revenue change |
| `halo___cannibal_impact` | Numeric | Measure — halo/cannibalisation impact |
| `own_price_impact` | Numeric | Measure — own-price elasticity impact |
| `prod_hier_sk` | Numeric | Dimension — product hierarchy key |
| `name` | String | Dimension — product name |
| `promotion_name` | String | Dimension — promotion name |
| `total_price_impact` | Numeric | Measure — total price impact |

### 3.3 EsriMapProvider

- **Type:** Map provider (not a CAS table)
- **URL:** `https://services.arcgisonline.com/ArcGIS/rest/services`
- **Service:** `Canvas/World_Light_Gray_Base`
- Used by the Geographic Effectiveness geo map visual.

---

## 4. Calculations & Business Logic

### 4.1 Aggregate Calculated Items

| Measure Name | ID | SAS Expression | Plain-English Meaning |
|---|---|---|---|
| **Marketing Pct of Sales** | bi78 | `div(aggregate(average,group,${bi25,raw}), aggregate(sum,group,${bi36,raw}))` | Average marketing budget divided by total sales. Expresses marketing spend as a percentage of sales revenue for each group. |
| **Sales per SQFT** | bi383 | `div(aggregate(sum,group,${bi36,raw}), aggregate(average,group,${bi45,raw}))` | Total sales divided by average store square footage. Measures sales efficiency per unit of store space. |
| **ROI** | bi477 | `div(minus(aggregate(sum,group,${bi36,raw}), aggregate(sum,group,${bi37,raw})), aggregate(sum,group,${bi37,raw}))` | (Total Sales − Total Cost) ÷ Total Cost. Classic return on investment: how much profit is generated per dollar of cost. |
| **Adjust ROI** | bi479 | `div(minus(aggregate(sum,group,${bi381,raw}), aggregate(sum,group,${bi478,raw})), aggregate(sum,group,${bi478,raw}))` | Same as ROI but uses the parameter-adjusted Sales and Cost values instead of raw values. Enables what-if scenario analysis. |
| **Actual vs Forecast** | bi2869 | `div(minus(aggregate(sum,group,${bi1352,raw}), aggregate(sum,group,${bi1354,raw})), aggregate(sum,group,${bi1354,raw}))` | (Actual Revenue − Expected Revenue) ÷ Expected Revenue. Shows how actual promotion results compare to the forecast — positive means outperformance. |
| **Actual vs Baseline** | bi2872 | `div(minus(aggregate(sum,group,${bi1352,raw}), aggregate(sum,group,${bi1353,raw})), aggregate(sum,group,${bi1353,raw}))` | (Actual Revenue − Baseline Revenue) ÷ Baseline Revenue. Measures the incremental lift from the promotion above the no-promotion baseline. |
| **Forecast vs Baseline** | bi3643 | `div(minus(aggregate(sum,group,${bi1354,raw}), aggregate(sum,group,${bi1353,raw})), aggregate(sum,group,${bi1353,raw}))` | (Expected Revenue − Baseline Revenue) ÷ Baseline Revenue. Shows the initially projected incremental impact of the promotion. |

### 4.2 Row-Level Calculated Items

| Measure Name | ID | SAS Expression | Plain-English Meaning |
|---|---|---|---|
| **Adjusted Sales** | bi381 | `cond(in(${bi31,binned},'US_MW','US_CS','US_AT','LATA','EU','ASIA'), times(${bi36,raw},plus(0.85,#{pr380})), times(${bi36,raw},plus(1,#{pr380})))` | If the region is one of US_MW, US_CS, US_AT, LATA, EU, or ASIA, multiply Sales by (0.85 + Sales Change parameter); otherwise multiply Sales by (1 + Sales Change parameter). This applies a 15% discount factor to certain regions, then adds the user-specified sales change %. |
| **Adjusted Sales Cost** | bi478 | `times(${bi37,raw},plus(1,#{pr424}))` | Multiply Cost by (1 + Cost Change parameter). Allows what-if scenario modelling of cost increases or decreases. |

### 4.3 Grouped Items (Custom Categories)

| Group Name | ID | Type | Plain-English Meaning |
|---|---|---|---|
| **Product Size** | bi3681 | GroupedItem | Categorises products into size buckets (e.g., Small, Large) based on product name patterns. Used on the Promotion Effectiveness crosstab. |
| **Product Age** | bi3698 | GroupedItem | Categorises products into age segments (e.g., Dog, Mature Dog, Puppy) based on product name patterns. Used on the Promotion Effectiveness crosstab. |
| **Product Health** | bi3854 | GroupedItem | Categorises products into health categories (e.g., Regular, Weight Control) based on product name patterns. Used on the Promotion Effectiveness crosstab. |

---

## 5. Visual Inventory

### 5.1 Section 0: Store Banner Dashboard

**Screenshot observations:** Three KPI cards across the top showing "Marketing Pct of Sales" for each banner (FAST: 14%, GRAND: 0.52%, MODA: 53%). Below each KPI is a dual-axis bar-line chart with teal bars for Sales (left axis, millions) and a dark blue line for Marketing Budget (right axis, billions). Departments are listed on the x-axis. The layout is a three-column comparison. MODA's health department dominates its sales but marketing budget is disproportionately high (53%). A footer text provides context and a "More information" link to the hidden section.

| Visual | Type | Chart Type | Measures | Dimensions | Filters | Business Question |
|---|---|---|---|---|---|---|
| Key Value – Marketing Pct of Sales 1 (ve97) | Graph | keyValue | Marketing Pct of Sales (bi78) | — | Storechain = 'GRAND' | What % of GRAND's sales goes to marketing? |
| Key Value – Marketing Pct of Sales 2 (ve141) | Graph | keyValue | Marketing Pct of Sales (bi78) | — | Storechain = 'MODA' | What % of MODA's sales goes to marketing? |
| Key Value – Marketing Pct of Sales 3 (ve73) | Graph | keyValue | Marketing Pct of Sales (bi78) | — | Storechain = 'FAST' | What % of FAST's sales goes to marketing? |
| Dual Axis Bar-Line – Merchandise Hierarchy 1 (ve166) | Graph | dualAxisBarLine | Sales (sum), Marketing Budget (mkt_bdgt) | Merchandise Hierarchy (Department/Class/brand) | Storechain = 'GRAND'; Top 5 by Sales per group | How do sales and marketing budget compare across GRAND's top 5 departments? |
| Dual Axis Bar-Line – Merchandise Hierarchy 2 (ve179) | Graph | dualAxisBarLine | Sales (sum), Marketing Budget (mkt_bdgt) | Merchandise Hierarchy (Department/Class/brand) | Storechain = 'MODA'; Top 5 by Sales per group | How do sales and marketing budget compare across MODA's top 5 departments? |
| Dual Axis Bar-Line – Merchandise Hierarchy 3 (ve152) | Graph | dualAxisBarLine | Sales (sum), Marketing Budget (mkt_bdgt) | Merchandise Hierarchy (Department/Class/brand) | Storechain = 'FAST'; Top 5 by Sales per group | How do sales and marketing budget compare across FAST's top 5 departments? |
| Store Banner Dashboard Text (ve55) | Text | — | — | — | — | Descriptive subtitle with "More information" navigation link |

### 5.2 Section 2: Geographic Effectiveness

**Screenshot observations:** Left side has a **Sales Change** slider prompt (range −100% to 100%, default 20%), and below it a horizontal bar chart titled "Sales by Region — Select a bar to filter the map". Seven U.S. regions are listed (US Atlantic Coast, US West Coast, US Midwest, US Southeast, US Southwest, US South Central, US Northeast). Each region has two bars: teal for Sales and dark purple for Adjusted Sales. US Atlantic Coast leads. Right side shows an **Esri choropleth map** of the continental U.S. titled "Map of Adjust ROI by State (darker color means higher ROI)". States are shaded in varying teal intensities. Some states (e.g., Montana, parts of the Midwest) appear lighter (lower ROI), while coastal and southern states appear darker (higher ROI). Grey states have no data.

| Visual | Type | Chart Type | Measures | Dimensions | Filters | Business Question |
|---|---|---|---|---|---|---|
| Sales Change Filter (ve7497) | Prompt | slider | — | — | Controls parameter `pr380` (Sales Change) | What-if: how would a sales increase/decrease change regional results? |
| Sales Region (ve428) | Graph | bar (horizontal) | Sales (sum), Adjusted Sales (bi381 sum) | Custom Region (bi6322) | US states only | How do actual and adjusted sales compare across U.S. regions? |
| ROI Region Map (ve371) | Graph | geo (Esri) | Adjust ROI (bi479) | State (bi362) | US states only | Which states have the highest adjusted ROI? |
| Text 2 (ve473) | Text | — | — | — | — | Page subtitle with "More information" navigation link |

### 5.3 Section 4: Promotion Effectiveness

**Screenshot observations:** Left side has a large **tree map** titled "Actual vs Baseline by Product Name — Select Product Name for additional information". Products are sized by actual revenue and shaded by Actual vs Baseline lift. The two largest tiles are "Iamz Dog Large Breed 1X17.4 LB" and "Iamz Dog Mini Chunks 1X17.4 LB". Upper right shows a **gauge/ring chart** titled "Promotional Sales Lift" displaying **0.68%** — the overall actual-vs-baseline lift. Below the gauge is a **crosstab** showing Product Age, Product Health, Product Size columns with Actual vs Baseline and Actual vs Forecast percentages. Rows include Dog/Weight Control/Small (44.88% lift), Dog/Regular/Large (10.82%), Puppy/Regular/Large (−12.12%, negative). Bottom left shows a small legend with Revenue Actual ($5.2K, $967 range) and a horizontal bar for Actual vs Baseline (0%–40%+ range).

| Visual | Type | Chart Type | Measures | Dimensions | Filters | Business Question |
|---|---|---|---|---|---|---|
| Sales vs Base Tree Map (ve2851) | Graph | treeMap | Revenue Actual (bi1352), Actual vs Baseline (bi2872) | Product Name (bi1360) | Filtered to 12 specific Iamz products | Which products generate the most revenue and the highest promotional lift? |
| Gauge 1 (ve7225) | Graph | keyValue (gauge) | Actual vs Baseline (bi2872) | — | Same 12-product filter | What is the overall promotional sales lift? |
| Promotion Details (ve7268) | Crosstab | — | Actual vs Baseline (bi2872), Actual vs Forecast (bi2869) | Product Age (bi3698), Product Health (bi3854), Product Size (bi3681) | Non-missing products only | How does promotional lift vary by product age, health category, and size? |
| Promotion Effectiveness Text (ve2983) | Text | — | — | — | — | Page subtitle with "More information" navigation link |

---

## 6. Interactivity & Navigation

### 6.1 Parameters / Prompts

| Parameter | ID | Label | Range | Default | Affects |
|---|---|---|---|---|---|
| **Sales Change** | pr380 | Sales Change | −100% to 100% | 20% (visible on screenshot) | `Adjusted Sales` (bi381) — applied as an additive factor to the sales multiplier. Also flows into `Adjust ROI` (bi479). |
| **Cost Change** | pr424 | Cost Change | Not visible in screenshots (likely similar slider) | Unknown | `Adjusted Sales Cost` (bi478) — applied as an additive factor to the cost multiplier. Also flows into `Adjust ROI` (bi479). |

### 6.2 Report-Level & Section-Level Filters

| Scope | Filter | Expression | Purpose |
|---|---|---|---|
| Section 0 — FAST charts | Storechain = 'FAST' | `in(${bi81/bi85,binned},'FAST')` | Isolate FAST banner data |
| Section 0 — GRAND charts | Storechain = 'GRAND' | `in(${bi86/bi148,binned},'GRAND')` or `or(in(…,'GRAND'),ismissing(…))` | Isolate GRAND banner data |
| Section 0 — MODA charts | Storechain = 'MODA' | `or(in(${bi148,binned},'MODA'),ismissing(…))` | Isolate MODA banner data |
| Section 2 — Bar & Map | US States only | `or(in(${bi7089/bi7966,binned},'AL','AR',…,'WV'),ismissing(…))` | Restrict to 41 U.S. states |
| Section 4 — Tree Map | 12 Iamz products | `in(${bi2856,binned},'Iamz Dog Active Maturity…',…)` | Focus on specific Iamz product lines |
| Section 4 — Crosstab | Non-missing | `notMissing(${bi7272,binned})` | Exclude rows with no product category |

### 6.3 Rank / Top-N Rules

| Visual | Rank By | Group By | N | Subset | Include Ties |
|---|---|---|---|---|---|
| Dual Axis Bar-Line (FAST) — dd153 | bi159 (Sales) | bi158 (Department) | 5 | Top | Yes |
| Dual Axis Bar-Line (GRAND) — dd167 | bi172 (Sales) | bi171 (Department) | 5 | Top | Yes |
| Dual Axis Bar-Line (MODA) — dd180 | bi185 (Sales) | bi184 (Department) | 5 | Top | Yes |

### 6.4 Navigation Actions

- Each visible section has a **"More information"** text link that navigates to its corresponding hidden information section (e.g., "Store Banner Dashboard" → "Store Banner Dashboard Information").
- The Merchandise Hierarchy in the bar-line charts supports **drill-down** (Department → Class → brand_name) via double-click.
- The bar chart on Geographic Effectiveness acts as a **cross-filter** for the Esri map ("Select a bar to filter the map").
- The tree map on Promotion Effectiveness acts as a **cross-filter** for the crosstab and gauge ("Select Product Name for additional information").

---

## 7. Migration Notes for Power BI

### 7.1 DAX Equivalents

| SAS Measure | DAX Equivalent |
|---|---|
| **Marketing Pct of Sales** | `Marketing Pct of Sales = DIVIDE( AVERAGE('RAND_RETAILDEMO'[mkt_bdgt]), SUM('RAND_RETAILDEMO'[Sales]) )` |
| **Sales per SQFT** | `Sales per SQFT = DIVIDE( SUM('RAND_RETAILDEMO'[Sales]), AVERAGE('RAND_RETAILDEMO'[sss]) )` |
| **ROI** | `ROI = DIVIDE( SUM('RAND_RETAILDEMO'[Sales]) - SUM('RAND_RETAILDEMO'[Cost]), SUM('RAND_RETAILDEMO'[Cost]) )` |
| **Adjusted Sales** | ⚠️ `Adjusted Sales = IF( 'RAND_RETAILDEMO'[Region_2] IN {"US_MW","US_CS","US_AT","LATA","EU","ASIA"}, 'RAND_RETAILDEMO'[Sales] * (0.85 + [Sales Change Value]), 'RAND_RETAILDEMO'[Sales] * (1 + [Sales Change Value]) )` — Requires a **What-If parameter** named `Sales Change Value` (numeric, −1 to 1, default 0.20). This is a calculated column or a measure using `SELECTEDVALUE` on the parameter table. |
| **Adjusted Sales Cost** | ⚠️ `Adjusted Sales Cost = 'RAND_RETAILDEMO'[Cost] * (1 + [Cost Change Value])` — Requires a **What-If parameter** named `Cost Change Value`. |
| **Adjust ROI** | `Adjust ROI = DIVIDE( SUM('RAND_RETAILDEMO'[Adjusted Sales]) - SUM('RAND_RETAILDEMO'[Adjusted Sales Cost]), SUM('RAND_RETAILDEMO'[Adjusted Sales Cost]) )` — ⚠️ If `Adjusted Sales` is implemented as a measure (not column), wrap sums in `SUMX`. |
| **Actual vs Forecast** | `Actual vs Forecast = DIVIDE( SUM('PROMO_EFFECTIVENESS'[revenue_____actual]) - SUM('PROMO_EFFECTIVENESS'[revenue_____expected]), SUM('PROMO_EFFECTIVENESS'[revenue_____expected]) )` |
| **Actual vs Baseline** | `Actual vs Baseline = DIVIDE( SUM('PROMO_EFFECTIVENESS'[revenue_____actual]) - SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]), SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]) )` |
| **Forecast vs Baseline** | `Forecast vs Baseline = DIVIDE( SUM('PROMO_EFFECTIVENESS'[revenue_____expected]) - SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]), SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]) )` |

### 7.2 Data Preparation Steps

| Item | Action Required |
|---|---|
| **Custom Region (bi6322)** | The SAS report applies a custom grouping to `Region`. Recreate as a calculated column in Power Query or DAX mapping region codes (e.g., `US_MW` → "US Midwest") to friendly names. |
| **Product Size / Product Age / Product Health** (GroupedItems) | These are custom-binned categories derived from product names. Implement as Power Query conditional columns or a lookup table mapping `name` → Size, Age, Health. The exact bin rules are embedded in the SAS report XML and should be extracted from the GroupedItem definitions. |
| **Merchandise Hierarchy** | Create a hierarchy in the Power BI data model: `Department` → `Class` → `brand_name`. Enable drill-down on bar charts. |
| **Transaction MMYYYY** | Use Power Query to format `Date` as `FORMAT([Date], "MM/YYYY")` or use a Date dimension table. |
| **Transaction Date DOW** | Use `FORMAT([Date], "dddd")` or a Date table's DayOfWeek column. |
| **State - Region** | Create a calculated column concatenating State and Region, or use a geography table. |
| **Column naming** | The PROMO table has unusual column names with multiple underscores (e.g., `revenue_____actual`). Clean these during import in Power Query. |

### 7.3 Potential Challenges

| Challenge | Mitigation |
|---|---|
| **Esri Map** | SAS VA uses an Esri ArcGIS map provider. Power BI has a built-in **ArcGIS Maps for Power BI** visual or the native **Shape Map** / **Filled Map**. The Esri integration requires an ArcGIS account in Power BI. Alternatively, use the native filled map with State as the geography field and Adjust ROI as the colour saturation value. |
| **What-If Parameters** | SAS VA uses slider prompts (`pr380`, `pr424`) that feed into row-level calculations. Power BI supports **What-If parameters** (creates a disconnected table + measure). However, the `Adjusted Sales` calculation conditionally applies a 15% discount by region, which requires a `SUMX` + `IF` pattern in DAX — this may be slower on large datasets. |
| **Top-N Rank Filters with Drill-Down** | SAS VA applies rank filters (Top 5) per hierarchy level with ties included. In Power BI, use the **Top N** filter pane or `TOPN` DAX function. When combined with hierarchy drill-down, test that the Top N re-evaluates at each drill level. |
| **Dual-Axis Bar-Line Charts** | Power BI's **Line and Clustered Column Chart** is the direct equivalent. Map Sales to columns (left axis) and Marketing Budget to the line (right axis). |
| **Cross-Filtering Behaviour** | SAS VA's "select a bar to filter the map" behaviour maps to Power BI's **Edit Interactions** feature (set bar chart → map to "Filter" instead of "Highlight"). |
| **Tree Map Sizing + Colour** | SAS VA tree map uses actual revenue for tile size and Actual vs Baseline for colour saturation. Power BI's tree map supports both **Values** (size) and a separate colour saturation field via conditional formatting. |
| **Hidden Information Sections** | SAS VA's hidden sections with explanatory text can be replicated as Power BI **Tooltip pages**, **Bookmarks with overlay text panels**, or a separate "Info" page with navigation buttons. |
| **Key Value / Gauge Visuals** | SAS VA's `keyValue` type maps to Power BI's **Card** visual (for single KPIs) or **Gauge** visual (for the ring gauge on the Promotion page). |
| **Date Handling** | Ensure the `Date` field is imported as a proper Date type and connect it to a Power BI Date dimension table for time intelligence functions. |

---

*Documentation generated from SAS Visual Analytics report analysis. All expressions, column names, and filter definitions are extracted directly from the report XML definition.*