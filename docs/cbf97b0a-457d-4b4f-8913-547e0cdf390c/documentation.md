# Report: Retail Insights

**Report ID:** `cbf97b0a-457d-4b4f-8913-547e0cdf390c`
**Last modified:** 2018-06-21T19:06:43.639Z
**Created by:** sas.SASVisualAnalytics
**Created on:** 2018-05-01T17:32:08.703Z

---

## 1. Executive Summary

**Business Domain:** Retail operations – store performance, geographic sales analysis, and promotional effectiveness.

**Target Audience:** Retail executives, marketing managers, regional sales directors, and merchandising analysts who need to evaluate store banner performance, geographic revenue patterns, and the ROI of promotional campaigns.

**Key Questions Answered:**

1. **Which store banner (FAST, GRAND, MODA) is performing best in terms of sales vs. marketing spend?** The Store Banner Dashboard compares three banners across departments and highlights where marketing budgets are (or aren't) translating into sales.
2. **Which geographic regions and states offer the best ROI, and how would adjusted sales forecasts change the picture?** The Geographic Effectiveness page lets analysts apply what-if sales and cost adjustments to evaluate regions for new store openings or improvements.
3. **Are promotions delivering incremental revenue above baseline, and how accurate are revenue forecasts?** The Promotion Effectiveness page compares actual, forecasted, and baseline revenue for specific product promotions.

---

## 2. Report Structure

The report has **6 sections total**: 3 visible analytical pages and 3 hidden information/help pages.

| # | Section Name | Type | Purpose |
|---|---|---|---|
| 0 | **Store Banner Dashboard** | Visible | Compares three store banners (FAST, GRAND, MODA) on Marketing % of Sales, Sales by department, and Marketing Budget. Uses KPI tiles and dual-axis bar-line charts. |
| 1 | Store Banner Dashboard Information | Hidden | Help/about text explaining how to interpret the Store Banner Dashboard. |
| 2 | **Geographic Effectiveness** | Visible | Analyzes sales and ROI by region/state on a geo map and bar chart. Includes slider prompts for what-if sales/cost adjustments. |
| 3 | Geographic Effectiveness Information | Hidden | Help/about text explaining how to use the Geographic Effectiveness page. |
| 4 | **Promotion Effectiveness** | Visible | Evaluates promotional performance via actual-vs-forecast-vs-baseline comparisons, tree maps, crosstabs, and gauge KPIs. |
| 5 | Promotion Effectiveness Information | Hidden | Help/about text explaining how to use the Promotion Effectiveness page. |

---

## 3. Data Model

### 3.1 Data Source: RAND_RETAILDEMO

| Property | Value |
|---|---|
| **Internal Name** | ds10 |
| **CAS Server** | cas-shared-default |
| **Library** | Samples |
| **Table** | RAND_RETAILDEMO |
| **Locale** | en_US |

**Key Columns:**

| Column (xref) | Internal ID | Role | Apparent Type | Notes |
|---|---|---|---|---|
| brand_name1 | bi11 | Dimension | Character | Brand name variant 1 |
| ChannelType | bi12 | Dimension | Character | Sales channel |
| City | bi13 | Dimension | Character | City name |
| City_Lat | bi14 | Measure (geo) | Numeric | City latitude |
| City_Long | bi15 | Measure (geo) | Numeric | City longitude |
| Country | bi16 | Dimension | Character | Country name |
| Country_Lat / Country_Long | bi17/bi18 | Measure (geo) | Numeric | Country coordinates |
| age | bi19 | Measure | Numeric | Customer age |
| age_bucket | bi20 | Dimension | Character | Age range bucket |
| CustID | bi21 | Dimension | Character | Customer identifier |
| loyalty_card | bi22 | Dimension | Character | Loyalty card flag |
| bucket | bi23 | Dimension | Character | Generic bucket |
| Margin | bi24 | Measure | Numeric | Profit margin |
| mkt_bdgt | bi25 | Measure | Numeric | Marketing budget |
| MDY | bi26 | Dimension | Character | Month-Day-Year string |
| brand_name | bi27 | Dimension | Character | Brand name |
| Class | bi28 | Dimension | Character | Product classification |
| Department | bi29 | Dimension | Character | Product department |
| Region | bi30 | Dimension | Character | Sales region |
| Region_2 | bi31 | Dimension | Character | Region code (US_MW, US_CS, etc.) |
| Region_2_Lat / Region_2_Long | bi32/bi33 | Measure (geo) | Numeric | Region_2 coordinates |
| Region_Lat / Region_Long | bi34/bi35 | Measure (geo) | Numeric | Region coordinates |
| Sales | bi36 | Measure | Numeric | Transaction sales amount |
| Cost | bi37 | Measure | Numeric | Transaction cost |
| State | bi38 | Dimension | Character | US State |
| State_Lat / State_Long | bi39/bi40 | Measure (geo) | Numeric | State coordinates |
| Storeage | bi41 | Measure | Numeric | Store age |
| Storechain | bi42 | Dimension | Character | Store chain name |
| Storechain1 | bi43 | Dimension | Character | Store chain variant |
| StoreNum | bi44 | Dimension | Character | Store number |
| sss | bi45 | Measure | Numeric | Store square footage |
| Date | bi46 | Dimension | Date | Transaction date |
| trx_dow_new | bi47 | Dimension | Character | Transaction day of week |
| trx_tod | bi49 | Dimension | Character | Transaction time of day |
| Year | bi50 | Dimension | Numeric | Transaction year |
| trx_hr_char | bi2419 | Dimension | Character | Transaction hour (character) |

**Derived/Custom Columns:**

| Column | Internal ID | Base Column | Notes |
|---|---|---|---|
| Transaction MMYYYY | bi234 | Date | Date formatted as MMYYYY |
| State - Region | bi362 | State | State with region context |
| Regular Sales Avg | bi382 | Sales | Average aggregation of Sales |
| Transaction Date DOW | bi1488 | Date | Day of week from Date |
| Custom Region | bi6322 | Region | Custom grouping of Region |

**Hierarchies:**

| Hierarchy | Internal ID | Purpose |
|---|---|---|
| Merchandise Hierarchy | bi157 | Drill path for product: likely Department → Class → Brand |
| Hierarchy 1 | bi6456 | Additional hierarchy (exact levels unknown) |
| Hierarchy 2 | bi6505 | Additional hierarchy (exact levels unknown) |

---

### 3.2 Data Source: PROMO_EFFECTIVENESS_X_EFFECTS_2

| Property | Value |
|---|---|
| **Internal Name** | ds1351 |
| **CAS Server** | cas-shared-default |
| **Library** | Samples |
| **Table** | PROMO_EFFECTIVENESS_X_EFFECTS_2 |
| **Locale** | en_US |

**Key Columns:**

| Column (xref) | Internal ID | Role | Apparent Type | Notes |
|---|---|---|---|---|
| revenue_____actual | bi1352 | Measure | Numeric | Actual revenue |
| revenue_____baseline | bi1353 | Measure | Numeric | Baseline revenue (no promotion) |
| revenue_____expected | bi1354 | Measure | Numeric | Forecasted/expected revenue |
| forecast_accuracy | bi1355 | Measure | Numeric | Forecast accuracy metric |
| revenue_____expected_change | bi1356 | Measure | Numeric | Expected change in revenue |
| halo___cannibal_impact | bi1357 | Measure | Numeric | Halo/cannibalization impact |
| own_price_impact | bi1358 | Measure | Numeric | Own-price impact on revenue |
| prod_hier_sk | bi1359 | Dimension | Numeric | Product hierarchy surrogate key |
| name | bi1360 | Dimension | Character | Product name |
| promotion_name | bi1361 | Dimension | Character | Promotion name |
| total_price_impact | bi1362 | Measure | Numeric | Total price impact |

---

### 3.3 Data Source: EsriMapProvider

| Property | Value |
|---|---|
| **Internal Name** | ds378 |
| **Type** | Map Provider (Esri) |
| **URL** | https://services.arcgisonline.com/ArcGIS/rest/services |
| **Service** | Canvas/World_Light_Gray_Base |

Used as the basemap tile layer for the Geographic Effectiveness geo chart.

---

## 4. Calculations & Business Logic

### 4.1 Calculated Measures (CalculatedItem & AggregateCalculatedItem)

| # | Measure Name | Type | SAS Expression | Plain-English Meaning |
|---|---|---|---|---|
| 1 | **Marketing Pct of Sales** | AggregateCalculatedItem | `div(aggregate(average,group,${bi25,raw}), aggregate(sum,group,${bi36,raw}))` | Average marketing budget divided by total sales within the current grouping. Answers: "What percentage of sales revenue goes to marketing?" |
| 2 | **Adjusted Sales** | CalculatedItem | `cond(in(${bi31,binned},'US_MW','US_CS','US_AT','LATA','EU','ASIA'), times(${bi36,raw},plus(0.85,#{pr380})), times(${bi36,raw},plus(1,#{pr380})))` | Row-level calculation: For six specific regions (US Midwest, US Central-South, US Atlantic, Latin America, Europe, Asia), Sales is multiplied by (0.85 + Sales Change slider value). For all other regions, Sales is multiplied by (1 + Sales Change slider value). This creates a what-if adjusted sales figure with a built-in 15% discount for the listed regions. |
| 3 | **Sales per SQFT** | AggregateCalculatedItem | `div(aggregate(sum,group,${bi36,raw}), aggregate(average,group,${bi45,raw}))` | Total sales divided by average store square footage. Measures sales productivity per unit of floor space. |
| 4 | **ROI** | AggregateCalculatedItem | `div(minus(aggregate(sum,group,${bi36,raw}), aggregate(sum,group,${bi37,raw})), aggregate(sum,group,${bi37,raw}))` | (Total Sales − Total Cost) / Total Cost. Standard return on investment calculation for each grouping. |
| 5 | **Adjusted Sales Cost** | CalculatedItem | `times(${bi37,raw},plus(1,#{pr424}))` | Row-level: Cost multiplied by (1 + Cost Change slider value). A what-if cost adjustment using the Cost Change parameter. |
| 6 | **Adjust ROI** | AggregateCalculatedItem | `div(minus(aggregate(sum,group,${bi381,raw}), aggregate(sum,group,${bi478,raw})), aggregate(sum,group,${bi478,raw}))` | (Total Adjusted Sales − Total Adjusted Cost) / Total Adjusted Cost. ROI recalculated using the what-if adjusted sales and adjusted cost values. |
| 7 | **Actual vs Forecast** | AggregateCalculatedItem | `div(minus(aggregate(sum,group,${bi1352,raw}), aggregate(sum,group,${bi1354,raw})), aggregate(sum,group,${bi1354,raw}))` | (Actual Revenue − Expected Revenue) / Expected Revenue. Measures how much actual promotional revenue exceeded or fell short of the forecast. |
| 8 | **Actual vs Baseline** | AggregateCalculatedItem | `div(minus(aggregate(sum,group,${bi1352,raw}), aggregate(sum,group,${bi1353,raw})), aggregate(sum,group,${bi1353,raw}))` | (Actual Revenue − Baseline Revenue) / Baseline Revenue. Measures the incremental lift of a promotion over the no-promotion baseline. |
| 9 | **Forecast vs Baseline** | AggregateCalculatedItem | `div(minus(aggregate(sum,group,${bi1354,raw}), aggregate(sum,group,${bi1353,raw})), aggregate(sum,group,${bi1353,raw}))` | (Forecasted Revenue − Baseline Revenue) / Baseline Revenue. Measures the projected incremental impact of a promotion. |

### 4.2 Grouped Items (Custom Groups)

| # | Group Name | Type | Purpose |
|---|---|---|---|
| 10 | **Product Size** | GroupedItem | Custom grouping of products by size (e.g., Small, Large). Exact bin definitions are in the XML but the group maps specific product names to size categories. |
| 11 | **Product Age** | GroupedItem | Custom grouping of products by age relevance (e.g., Puppy, Adult, Maturity). |
| 12 | **Product Health** | GroupedItem | Custom grouping of products by health/wellness positioning (e.g., Weight Control, Healthy Natural). |

---

## 5. Visual Inventory

### 5.1 Section: Store Banner Dashboard (Index 0)

**Screenshot Observations:** The page has a clean three-column layout. Each column represents one store banner (FAST, GRAND, MODA). At the top of each column is a large KPI tile showing "Marketing Pct of Sales" in teal text. Below each KPI is a dual-axis bar-line chart with teal bars representing Sales (millions, left axis) and a dark blue/navy line representing Marketing Budget (billions, right axis), broken down by department. A descriptive text box appears at the bottom.

**Key Observation from Screenshot:** MODA's Marketing Pct of Sales is 53% — dramatically higher than FAST (14%) and GRAND (0.52%). MODA shows disproportionate marketing spend relative to sales, with only Health and Grocery departments visible. FAST and GRAND have more departments visible (women, grocery, electronics, health, kids, men).

| Visual | Type | Chart Type | Measures | Dimensions | Filters | Business Question |
|---|---|---|---|---|---|---|
| Key Value - Marketing Pct of Sales 3 (ve73) | Graph | keyValue | Marketing Pct of Sales | — | Storechain = 'FAST' | What % of FAST banner sales goes to marketing? |
| Key Value - Marketing Pct of Sales 1 (ve97) | Graph | keyValue | Marketing Pct of Sales | — | Storechain = 'GRAND' | What % of GRAND banner sales goes to marketing? |
| Key Value - Marketing Pct of Sales 2 (ve141) | Graph | keyValue | Marketing Pct of Sales | — | Storechain = 'MODA' | What % of MODA banner sales goes to marketing? |
| Dual Axis Bar-Line - Merchandise Hierarchy 3 (ve152) | Graph | dualAxisBarLine | Sales, Marketing Budget | Department (via Merchandise Hierarchy) | Storechain = 'FAST'; Top 5 rank by Sales | How do Sales and Marketing Budget compare across departments for FAST? |
| Dual Axis Bar-Line - Merchandise Hierarchy 1 (ve166) | Graph | dualAxisBarLine | Sales, Marketing Budget | Department (via Merchandise Hierarchy) | Storechain = 'GRAND'; Top 5 rank by Sales | How do Sales and Marketing Budget compare across departments for GRAND? |
| Dual Axis Bar-Line - Merchandise Hierarchy 2 (ve179) | Graph | dualAxisBarLine | Sales, Marketing Budget | Department (via Merchandise Hierarchy) | Storechain = 'MODA'; Top 5 rank by Sales | How do Sales and Marketing Budget compare across departments for MODA? |
| Store Banner Dashboard Text (ve55) | Text | — | — | — | — | Descriptive guidance: "View high-level metrics based on each banner's performance…" |

**Rank Filters on this section:** Each bar-line chart applies a **Top 5** rank filter on Sales, grouped by a hierarchy level, with ties included. This limits each chart to the top 5 performing categories.

---

### 5.2 Section: Geographic Effectiveness (Index 2)

**Screenshot:** Not available via API. Based on structural data:

The page contains a geo map (Esri basemap), a bar chart for regional sales, a slider prompt for what-if analysis, and supporting KPI gauges. A text box provides context.

| Visual | Type | Chart Type | Measures | Dimensions | Filters | Business Question |
|---|---|---|---|---|---|---|
| ROI Region Map (ve371) | Graph | geo (Esri) | Adjust ROI (color) | State (geography) | US states only filter | Which US states have the best/worst adjusted ROI? |
| Sales Region (ve428) | Graph | bar | Sales, Adjusted Sales | Region | US states only filter | How do regular vs. adjusted sales compare by region? |
| Gauge 1 (ve7225) | Graph | keyValue | (likely Adjust ROI or Sales per SQFT) | — | — | What is the overall adjusted performance metric? |
| Sales Change Filter (ve7497) | Prompt | slider | — | — | Controls #{pr380} parameter | Lets users adjust the sales change % to model what-if scenarios |
| Text 2 (ve473) | Text | — | — | — | — | "Identify the performance of various regions and states…" |

**Interactivity:** The slider prompt controls the `Sales Change` (pr380) parameter, which feeds into the `Adjusted Sales` and `Adjust ROI` calculations. The geo map likely supports hover tooltips showing state-level metrics. The bar chart and map are filtered to US states only (42 state abbreviations enumerated in the filter expression).

---

### 5.3 Section: Promotion Effectiveness (Index 4)

**Screenshot:** Not available via API. Based on structural data:

The page contains a tree map showing product-level sales comparisons, a crosstab for promotion details, KPI gauges for actual-vs-forecast/baseline metrics, and descriptive text.

| Visual | Type | Chart Type | Measures | Dimensions | Filters | Business Question |
|---|---|---|---|---|---|---|
| Sales vs Base Tree Map (ve2851) | Graph | treeMap | Actual vs Baseline (color/size) | Product name | Filtered to 12 specific Iamz dog food products | Which products had the biggest promotional lift over baseline? |
| Promotion Details (ve7268) | Crosstab | — | Multiple revenue measures, impacts | Product Size, Product Age, Product Health (grouped items) | Filter: notMissing on a grouping field | What are the detailed promotion metrics by product attributes? |
| Gauge 1 (ve7225) | Graph | keyValue | Actual vs Forecast or Actual vs Baseline | — | — | What is the overall promotion performance KPI? |
| Promotion Effectiveness Text (ve2983) | Text | — | — | — | — | "Assess the success of promotions by comparing actual revenue to forecast and baseline revenue…" |

**Product Filter:** The tree map is filtered to 12 specific Iamz-brand dog food products (e.g., "Iamz Puppy Large Breed 1X17.4 LB", "Iamz Dog Active Maturity 1X17.4 LB", etc.).

---

## 6. Interactivity & Navigation

### 6.1 Parameters / Prompts

| Parameter | Internal ID | Label | Default | Used In | Effect |
|---|---|---|---|---|---|
| **Sales Change** | pr380 | Sales Change | Not specified (likely 0) | `Adjusted Sales` (bi381) | Slider value added to the regional multiplier. Positive values increase adjusted sales; negative values decrease them. For 6 specific regions, the base multiplier starts at 0.85 (a 15% penalty); for others it starts at 1.0. |
| **Cost Change** | pr424 | Cost Change | Not specified (likely 0) | `Adjusted Sales Cost` (bi478) | Slider value added to 1.0, then multiplied by Cost. Allows modeling cost increases/decreases. |

### 6.2 Detail Filters (Section-Level)

| Section | Visual | Filter Logic |
|---|---|---|
| Store Banner Dashboard | FAST KPI & chart | `Storechain = 'FAST'` |
| Store Banner Dashboard | GRAND KPI & chart | `Storechain = 'GRAND'` |
| Store Banner Dashboard | MODA KPI & chart | `Storechain = 'MODA'` |
| Geographic Effectiveness | ROI Region Map | State in 42 enumerated US state codes, or missing |
| Geographic Effectiveness | Sales Region bar | State in 42 enumerated US state codes, or missing |
| Promotion Effectiveness | Tree Map | Product name in 12 specific Iamz products |
| Promotion Effectiveness | Crosstab | `notMissing` on a grouped dimension |

### 6.3 Rank / Top-N Filters

| Visual | Rank | Group By | Order | Include Ties |
|---|---|---|---|---|
| FAST Dual Axis Bar-Line (ve152) | Top 5 | Hierarchy level (bi158) | By Sales (bi159) descending | Yes |
| GRAND Dual Axis Bar-Line (ve166) | Top 5 | Hierarchy level (bi171) | By Sales (bi172) descending | Yes |
| MODA Dual Axis Bar-Line (ve179) | Top 5 | Hierarchy level (bi184) | By Sales (bi185) descending | Yes |

### 6.4 Navigation Actions

The hidden information sections (indices 1, 3, 5) serve as help/about overlays. Each visible section has a "More information" link in its text box that likely navigates to the corresponding hidden information section. This is a common SAS VA pattern using NavigationAction elements.

### 6.5 Drill-Down

The Merchandise Hierarchy (bi157) enables drill-down on the Store Banner Dashboard bar-line charts. Users can double-click a department bar to drill into the top 5 classifications, and further into individual brands/classes.

---

## 7. Migration Notes for Power BI

### 7.1 DAX Equivalents for Calculated Measures

| SAS Measure | DAX Equivalent | Notes |
|---|---|---|
| **Marketing Pct of Sales** | `Marketing Pct of Sales = DIVIDE(AVERAGE('RAND_RETAILDEMO'[mkt_bdgt]), SUM('RAND_RETAILDEMO'[Sales]))` | Direct translation. |
| **Sales per SQFT** | `Sales per SQFT = DIVIDE(SUM('RAND_RETAILDEMO'[Sales]), AVERAGE('RAND_RETAILDEMO'[sss]))` | Direct translation. `sss` = store square footage. |
| **ROI** | `ROI = DIVIDE(SUM('RAND_RETAILDEMO'[Sales]) - SUM('RAND_RETAILDEMO'[Cost]), SUM('RAND_RETAILDEMO'[Cost]))` | Direct translation. |
| **Adjusted Sales** | ⚠️ `Adjusted Sales = SUMX('RAND_RETAILDEMO', IF('RAND_RETAILDEMO'[Region_2] IN {"US_MW","US_CS","US_AT","LATA","EU","ASIA"}, 'RAND_RETAILDEMO'[Sales] * (0.85 + [Sales Change Value]), 'RAND_RETAILDEMO'[Sales] * (1 + [Sales Change Value])))` | Requires a **What-If Parameter** for `Sales Change Value`. The SAS expression is row-level (`CalculatedItem`) so `SUMX` is needed. The 6 regions get a 15% base discount. |
| **Adjusted Sales Cost** | ⚠️ `Adjusted Sales Cost = SUMX('RAND_RETAILDEMO', 'RAND_RETAILDEMO'[Cost] * (1 + [Cost Change Value]))` | Requires a **What-If Parameter** for `Cost Change Value`. |
| **Adjust ROI** | ⚠️ `Adjust ROI = DIVIDE([Adjusted Sales] - [Adjusted Sales Cost], [Adjusted Sales Cost])` | Depends on the two what-if measures above. |
| **Actual vs Forecast** | `Actual vs Forecast = DIVIDE(SUM('PROMO_EFFECTIVENESS'[revenue_____actual]) - SUM('PROMO_EFFECTIVENESS'[revenue_____expected]), SUM('PROMO_EFFECTIVENESS'[revenue_____expected]))` | Direct translation. |
| **Actual vs Baseline** | `Actual vs Baseline = DIVIDE(SUM('PROMO_EFFECTIVENESS'[revenue_____actual]) - SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]), SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]))` | Direct translation. |
| **Forecast vs Baseline** | `Forecast vs Baseline = DIVIDE(SUM('PROMO_EFFECTIVENESS'[revenue_____expected]) - SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]), SUM('PROMO_EFFECTIVENESS'[revenue_____baseline]))` | Direct translation. |

### 7.2 Data Preparation Steps

1. **CAS to Power BI Data Source:** Both CAS tables (`Samples.RAND_RETAILDEMO` and `Samples.PROMO_EFFECTIVENESS_X_EFFECTS_2`) must be exported or connected. Options:
   - Export to CSV/Parquet and import into Power BI
   - Use a gateway connection if CAS supports ODBC/JDBC
   - Load into Azure SQL / Fabric Lakehouse

2. **Custom Region Grouping (bi6322):** The `Custom Region` column is derived from `Region`. The exact mapping must be extracted from the XML or recreated in Power Query as a conditional column.

3. **Grouped Items (Product Size, Product Age, Product Health):** These are custom groups defined in SAS VA on the PROMO_EFFECTIVENESS table. The group-to-product mappings must be extracted and implemented as:
   - Calculated columns in DAX, or
   - Conditional columns in Power Query, or
   - A separate lookup/mapping table

4. **Date Formatting:** `Transaction MMYYYY` (bi234) and `Transaction Date DOW` (bi1488) are custom date format columns. Recreate using Power Query date formatting or DAX `FORMAT()` function.

5. **State - Region (bi362):** A derived column combining State with Region context. Implement as a calculated column: `State - Region = 'RAND_RETAILDEMO'[State] & " - " & 'RAND_RETAILDEMO'[Region]` (exact format TBD from data inspection).

### 7.3 What-If Parameters

Power BI supports What-If Parameters natively:
- Create a **Sales Change** parameter (e.g., range -0.50 to +0.50 in 0.01 increments, default 0)
- Create a **Cost Change** parameter (e.g., range -0.50 to +0.50 in 0.01 increments, default 0)
- Both generate a disconnected table with a slicer; the selected value is referenced in DAX measures

### 7.4 Potential Challenges

| Challenge | Severity | Mitigation |
|---|---|---|
| **Esri Geo Map** | 🟡 Medium | Power BI has built-in ArcGIS Maps visual (requires Esri license) or can use the native Map/Shape Map visual. The World Light Gray Canvas basemap can be approximated with Mapbox in Power BI. |
| **Dual Axis Bar-Line Chart** | 🟢 Low | Power BI's Line and Clustered Column Chart provides identical functionality. |
| **Top-N Rank Filters with Drill-Down** | 🟡 Medium | Power BI supports Top N filtering in the visual-level filter pane. However, combining Top N with drill-down hierarchies requires careful configuration — the Top N filter must be re-applied at each hierarchy level, or use a RANKX DAX measure. |
| **Merchandise Hierarchy Drill-Down** | 🟡 Medium | Power BI supports hierarchy drill-down natively. Define a hierarchy: Department → Class → Brand in the data model. Ensure the Top-5 filter works at each level. |
| **Row-Level Conditional Calculations (Adjusted Sales)** | 🟡 Medium | The SAS `cond(in(...))` pattern requires `SUMX` with `IF` in DAX, which may be slower on large datasets. Consider creating a pre-computed column if performance is an issue. |
| **Hidden Info Sections as Help Pages** | 🟢 Low | Implement as Power BI tooltips, bookmarks with overlay text boxes, or a separate "Help" page with navigation buttons. |
| **NavigationAction (More Information links)** | 🟢 Low | Use Power BI bookmarks with buttons, or page navigation buttons. |
| **Grouped Items (Product Size/Age/Health)** | 🟡 Medium | The exact group mappings need to be extracted from the SAS VA XML `<GroupedItem>` definitions. Without the explicit mappings, these cannot be automatically recreated. Manual inspection of the XML or the SAS VA interface is needed. |
| **Filter Expressions with `ismissing` Logic** | 🟢 Low | SAS `ismissing()` → DAX `ISBLANK()`. Several filters use `or(in(...), ismissing(...))` which means "include selected values OR blanks." Replicate with `ISBLANK` in Power BI filter expressions. |
| **Column Naming Conventions** | 🟢 Low | Some CAS column names contain unusual characters (e.g., `revenue_____actual` with multiple underscores, `halo___cannibal_impact`). Clean these up during import for readability. |

### 7.5 Recommended Power BI Page Layout

| Power BI Page | Source Section | Visuals |
|---|---|---|
| **Page 1: Store Banner Dashboard** | Store Banner Dashboard | 3 × Card visuals (Marketing Pct of Sales per banner), 3 × Line and Clustered Column charts (Sales vs Marketing Budget by Department), Text box |
| **Page 2: Geographic Effectiveness** | Geographic Effectiveness | Map visual (Adjusted ROI by State), Clustered Bar chart (Sales by Region), What-If Parameter slicers (Sales Change, Cost Change), Card/Gauge visual |
| **Page 3: Promotion Effectiveness** | Promotion Effectiveness | Treemap (Actual vs Baseline by Product), Matrix/Table (Promotion Details by Product Size/Age/Health), Card visuals (Actual vs Forecast, Actual vs Baseline, Forecast vs Baseline) |
| **Help Overlays** | Hidden Sections | Implement as bookmark overlays triggered by ℹ️ info buttons on each page |

---

## Appendix: Complete Element Reference

### All Visual Elements by Section

| Section | Visual Name | Element ID | Type | Chart Subtype |
|---|---|---|---|---|
| Store Banner Dashboard | Key Value - Marketing Pct of Sales 3 | ve73 | Graph | keyValue |
| Store Banner Dashboard | Key Value - Marketing Pct of Sales 1 | ve97 | Graph | keyValue |
| Store Banner Dashboard | Key Value - Marketing Pct of Sales 2 | ve141 | Graph | keyValue |
| Store Banner Dashboard | Dual Axis Bar-Line - Merchandise Hierarchy 3 | ve152 | Graph | dualAxisBarLine |
| Store Banner Dashboard | Dual Axis Bar-Line - Merchandise Hierarchy 1 | ve166 | Graph | dualAxisBarLine |
| Store Banner Dashboard | Dual Axis Bar-Line - Merchandise Hierarchy 2 | ve179 | Graph | dualAxisBarLine |
| Store Banner Dashboard | Store Banner Dashboard Text | ve55 | Text | — |
| Geographic Effectiveness | ROI Region Map | ve371 | Graph | geo |
| Geographic Effectiveness | Sales Region | ve428 | Graph | bar |
| Geographic Effectiveness | Gauge 1 | ve7225 | Graph | keyValue |
| Geographic Effectiveness | Sales Change Filter | ve7497 | Prompt | slider |
| Geographic Effectiveness | Text 2 | ve473 | Text | — |
| Promotion Effectiveness | Sales vs Base Tree Map | ve2851 | Graph | treeMap |
| Promotion Effectiveness | Promotion Details | ve7268 | Crosstab | — |
| Promotion Effectiveness | Promotion Effectiveness Text | ve2983 | Text | — |

### All Filter Expressions

| Filter ID | Expression | Plain English |
|---|---|---|
| bi162 | `or(in(${bi161,binned},'FAST'),ismissing(${bi161,binned}))` | Include rows where Storechain is 'FAST' or is blank |
| bi175 | `or(in(${bi174,binned},'GRAND'),ismissing(${bi174,binned}))` | Include rows where Storechain is 'GRAND' or is blank |
| bi188 | `or(in(${bi187,binned},'MODA'),ismissing(${bi187,binned}))` | Include rows where Storechain is 'MODA' or is blank |
| bi82 | `in(${bi81,binned},'FAST')` | Include rows where Storechain is 'FAST' (strict, no blanks) |
| bi86 | `in(${bi85,binned},'GRAND')` | Include rows where Storechain is 'GRAND' (strict, no blanks) |
| bi149 | `or(in(${bi148,binned},'MODA'),ismissing(${bi148,binned}))` | Include rows where Storechain is 'MODA' or is blank |
| bi7088 | `or(in(${bi7966,binned},'AL','AR',...,'WV'),ismissing(...))` | Include rows for 42 specific US states or blanks |
| bi7090 | `or(in(${bi7089,binned},'AL','AR',...,'WV'),ismissing(...))` | Same US-states filter for the bar chart |
| bi4696 | `in(${bi2856,binned},'Iamz Dog Active Maturity...',...)` | Include only 12 specific Iamz dog food products |
| bi7287 | `notMissing(${bi7272,binned})` | Exclude rows where the grouped field is blank |

---

*Generated by SAS VA Documentation Agent | Report: Retail Insights | ID: cbf97b0a-457d-4b4f-8913-547e0cdf390c*