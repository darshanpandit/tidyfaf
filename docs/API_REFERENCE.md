# tidyfaf API Reference

Complete reference for the tidyfaf package.

## Table of Contents

1. [Query Classes](#query-classes)
2. [Discovery Functions](#discovery-functions)
3. [Utility Functions](#utility-functions)
4. [Data Schema](#data-schema)

---

## Query Classes

### FAFQuery

**Purpose**: Query regional zone-level freight flows (132 FAF zones).

**Constructor**:
```python
query = faf.FAFQuery()
```

#### Filter Methods

All filter methods return a new `FAFQuery` instance (immutable pattern).

##### Geography Filters

**`.origin_states(states: list)`**
- Filter by origin states
- Accepts: State names or FIPS codes
- Example: `.origin_states(['California', 'Texas'])` or `.origin_states([6, 48])`

**`.destination_states(states: list)`**
- Filter by destination states
- Accepts: State names or FIPS codes
- Example: `.destination_states(['Washington', 'Oregon'])`

**`.origin_zones(zones: list)`**
- Filter by origin FAF zones
- Accepts: Zone names or codes
- Example: `.origin_zones([111, 121])` or `.origin_zones(['Birmingham AL'])`

**`.destination_zones(zones: list)`**
- Filter by destination FAF zones
- Accepts: Zone names or codes
- Example: `.destination_zones([111])`

##### Commodity and Mode Filters

**`.commodities(commodities: list)`**
- Filter by commodities
- Accepts: Commodity names or SCTG2 codes
- Example: `.commodities(['Electronics', 'Pharmaceuticals'])` or `.commodities([35, 21])`

**`.modes(modes: list)`**
- Filter by transportation modes
- Accepts: Mode names or codes
- Valid names: 'Truck', 'Rail', 'Water', 'Air', 'Multiple/Intermodal', 'Pipeline', 'Other', 'Unknown'
- Example: `.modes(['Truck', 'Rail'])` or `.modes([1, 2])`

##### Temporal Filters

**`.years(years: list)`**
- Select specific years
- Accepts: List of years
- Valid years: 2017-2024 (actual), 2030-2050 in 5-year intervals (forecast)
- Example: `.years([2020, 2024, 2030])`

**`.year_range(start: int, end: int)`**
- Select year range (inclusive)
- Automatically handles gap between 2024 and 2030
- Example: `.year_range(2020, 2030)` → [2020, 2021, 2022, 2023, 2024, 2030]

##### Other Filters

**`.trade_types(trade_types: list)`**
- Filter by trade type
- Accepts: 'Domestic', 'Import', 'Export' or codes [1, 2, 3]
- Example: `.trade_types(['Domestic', 'Import'])`

**`.min_tons(value: float, year: int = 2020)`**
- Filter flows above tonnage threshold
- Example: `.min_tons(1000, year=2020)`

**`.min_value(value: float, year: int = 2020)`**
- Filter flows above value threshold (millions of dollars)
- Example: `.min_value(100, year=2020)`

#### Execution Methods

**`.get(format: str = 'wide')`**
- Execute query and return DataFrame
- `format='wide'`: Year columns (tons_2020, tons_2025, ...)
- `format='long'`: Tidy format with year as dimension
- Returns: `pd.DataFrame`

**`.to_gdf()`**
- Execute query and return GeoDataFrame with flow LineStrings
- Returns: `gpd.GeoDataFrame` with geometry in EPSG:4326

#### Analysis Methods

**`.group_by(fields: list|str, metrics: list = ['tons', 'value'], years: list = None)`**
- Group by specified fields and aggregate
- `fields`: Column names to group by (e.g., ['dms_orig', 'sctg2'])
- `metrics`: Metrics to aggregate ('tons', 'value', 'tmiles')
- `years`: Years to include (defaults to filtered years)
- Returns: `pd.DataFrame`

**`.by_origin(metrics: list = None, years: list = None)`**
- Shortcut for grouping by origin zone
- Returns: `pd.DataFrame`

**`.by_destination(metrics: list = None, years: list = None)`**
- Shortcut for grouping by destination zone
- Returns: `pd.DataFrame`

**`.by_commodity(metrics: list = None, years: list = None)`**
- Shortcut for grouping by commodity
- Returns: `pd.DataFrame`

**`.by_mode(metrics: list = None, years: list = None)`**
- Shortcut for grouping by mode
- Returns: `pd.DataFrame`

**`.top(n: int = 10, by: str = 'tons', year: int = 2020)`**
- Get top N flows by metric
- Returns: `pd.DataFrame`

**`.summarize(metric: str = 'tons', year: int = 2020)`**
- Quick summary statistics
- Returns: `dict` with keys: total, mean, median, min, max, flows

**`.compare_years(years: list = None)`**
- Year-over-year comparison
- Returns: `pd.DataFrame`

#### Discovery Methods

**`.available_commodities(search: str = None)`**
- List/search available commodities
- Returns: `pd.DataFrame` with commodity codes and descriptions

**`.available_zones(search: str = None)`**
- List/search available FAF zones
- Returns: `pd.DataFrame` with zone codes and descriptions

**`.available_states(search: str = None)`**
- List/search available states
- Returns: `pd.DataFrame` with state codes and names

**`.available_modes()`**
- List available modes
- Returns: `pd.DataFrame` with mode codes and descriptions

**`.available_years()`**
- List available years
- Returns: `dict` with keys: 'actual', 'forecast'

#### Utility Methods

**`.validate()`**
- Validate filter inputs
- Returns: `dict` with 'valid' (bool) and 'warnings' (list)

**`.estimate_size()`**
- Estimate result row count without executing full query
- Returns: `int`

---

### StateQuery

**Purpose**: Query state-level freight flows (faster, less granular).

**Constructor**:
```python
query = faf.StateQuery()
```

**Inherits from**: `FAFQuery`

**Key Differences**:
- Uses `dms_origst`/`dms_destst` columns instead of `dms_orig`/`dms_dest`
- Does NOT support `.origin_zones()` or `.destination_zones()` methods
- Does NOT support `.to_gdf()` (no zone geometries)
- All other methods work identically

**Example**:
```python
query = (faf.StateQuery()
    .origin_states(['California'])
    .destination_states(['Texas', 'Oregon'])
    .commodities(['Electronics'])
    .years([2020, 2030])
)
df = query.get()
```

---

### NetworkQuery

**Purpose**: Query FAF5 highway network.

**Constructor**:
```python
query = faf.NetworkQuery()
```

#### Filter Methods

**`.routes(routes: list)`**
- Filter by route names/numbers
- Example: `.routes(['I-5', 'I-95', 'US-101'])`

**`.states(states: list)`**
- Filter by state abbreviations or FIPS codes
- Example: `.states(['CA', 'OR', 'WA'])`

**`.zones(zones: list)`**
- Filter by FAF zone codes
- Example: `.zones([61, 62])`

**`.functional_classes(classes: list)`**
- Filter by functional class descriptions
- Valid values: 'Interstate', 'Arterial', 'Freeway', etc.
- Example: `.functional_classes(['Interstate'])`

**`.freight_network(nhfn: bool = True)`**
- Filter to National Highway Freight Network
- Example: `.freight_network(True)`

**`.nhs(nhs: bool = True)`**
- Filter to National Highway System
- Example: `.nhs(True)`

**`.truck_allowed(allowed: bool = True)`**
- Filter by truck access (excludes prohibited segments)
- Example: `.truck_allowed(True)`

**`.toll_roads(toll: bool = True)`**
- Filter by toll status
- Example: `.toll_roads(True)` (only toll roads) or `.toll_roads(False)` (exclude tolls)

#### Execution Methods

**`.get(format: str = 'wide')`**
- Execute query and return GeoDataFrame
- Returns: `gpd.GeoDataFrame` with LineString geometry

#### Analysis Methods

**`.total_length()`**
- Calculate total length of filtered segments
- Returns: `float`

**`.by_state()`**
- Group by state
- Returns: `pd.DataFrame` with STATE and LENGTH columns

**`.by_functional_class()`**
- Group by functional class
- Returns: `pd.DataFrame` with Class_Description and LENGTH columns

**`.by_zone()`**
- Group by FAF zone
- Returns: `pd.DataFrame` with FAFZONE and LENGTH columns

**`.summarize()`**
- Quick summary statistics
- Returns: `dict` with keys: total_segments, total_length, avg_length, states, functional_classes

**Example**:
```python
network = (faf.NetworkQuery()
    .routes(['I-5'])
    .states(['CA', 'OR', 'WA'])
    .freight_network(True)
)
gdf = network.get()
print(f"Total length: {network.total_length():,.0f} miles")
```

---

### ForecastQuery

**Purpose**: Query HiLo forecast scenarios (base/high/low).

**Constructor**:
```python
query = faf.ForecastQuery()
```

**Inherits from**: `FAFQuery`

#### Additional Methods

**`.scenarios(scenarios: list)`**
- Select forecast scenarios to include
- Valid values: 'base', 'high', 'low'
- Example: `.scenarios(['base', 'high', 'low'])`

**`.get(format: str = 'long')`**
- Execute query (default format is 'long' with scenario column)
- Returns: `pd.DataFrame` with 'scenario' column

**`.compare_scenarios(year: int = 2030)`**
- Compare base/high/low scenarios for specific year
- Returns: `pd.DataFrame`

**Example**:
```python
forecast = (faf.ForecastQuery()
    .origin_states(['California'])
    .commodities(['Electronics'])
    .years([2030, 2040, 2050])
    .scenarios(['base', 'high', 'low'])
)
df = forecast.get()
# df has columns: origin, dest, commodity, year, scenario, tons, value
```

---

## Discovery Functions

These are global functions available directly from `faf.*`:

**`faf.available_commodities(search: str = None)`**
- List/search all commodities
- Example: `faf.available_commodities(search='electronics')`
- Returns: `pd.DataFrame`

**`faf.available_zones(search: str = None)`**
- List/search all FAF zones
- Example: `faf.available_zones(search='washington')`
- Returns: `pd.DataFrame`

**`faf.available_states(search: str = None)`**
- List/search all states
- Example: `faf.available_states(search='california')`
- Returns: `pd.DataFrame`

**`faf.available_modes()`**
- List all transportation modes
- Example: `faf.available_modes()`
- Returns: `pd.DataFrame`

---

## Utility Functions

**`faf.clear_cache()`**
- Clear query result cache (keeps raw data cache)
- Example: `faf.clear_cache()`

**`faf.clear_all_caches()`**
- Clear all caches including raw data
- Example: `faf.clear_all_caches()`

**`faf.download_and_process()`**
- Download and process FAF5 data files
- First-time setup (downloads ~2GB)
- Example: `faf.download_and_process()`

---

## Visualization

**`FlowMap`**

Class for creating interactive Deck.gl flow maps.

```python
from tidyfaf.visualization import FlowMap

# Get flows as GeoDataFrame
gdf = query.to_gdf()

# Create map
fm = FlowMap(gdf)
fm.generate_map('output.html', flow_column='tons_2020', max_flows=1000)
```

**Parameters**:
- `gdf`: GeoDataFrame with flow LineStrings
- `flow_column`: Column to use for flow width
- `max_flows`: Maximum number of flows to display (for performance)

---

## Common Patterns

### Pattern 1: Multi-Level Geography

```python
# Origin: States, Destination: Specific zones
query = (faf.FAFQuery()
    .origin_states(['California', 'Texas'])
    .destination_zones([111, 121])  # DC, Baltimore
    .commodities(['Electronics'])
    .years([2020])
)
```

### Pattern 2: Iterative Refinement

```python
# Build base query
base = faf.FAFQuery().origin_states(['California'])

# Branch for different analyses
electronics = base.commodities(['Electronics']).get()
pharma = base.commodities(['Pharmaceuticals']).get()
```

### Pattern 3: Time Series Analysis

```python
# Get data in long format for time series
query = (faf.FAFQuery()
    .origin_states(['California'])
    .commodities(['Electronics'])
    .year_range(2017, 2030)
)

df_long = query.get(format='long')
# df_long has columns: origin, dest, commodity, year, tons, value, tmiles

# Calculate year-over-year growth
growth = df_long.groupby('year')['tons'].sum().pct_change()
```

### Pattern 4: Top OD Pairs with Details

```python
query = (faf.FAFQuery()
    .origin_states(['California'])
    .commodities(['Electronics'])
    .years([2020])
)

# Get top flows
top = query.top(10, by='tons', year=2020)

# Get summary stats
stats = query.summarize(metric='tons', year=2020)
print(f"Total: {stats['total']:,.0f} tons across {stats['flows']} flows")
```

### Pattern 5: Scenario Comparison

```python
forecast = (faf.ForecastQuery()
    .origin_states(['California'])
    .destination_states(['Texas'])
    .years([2030])
    .scenarios(['base', 'high', 'low'])
)

df = forecast.get(format='long')

# Pivot to compare scenarios
comparison = df.pivot_table(
    index=['dms_orig', 'dms_dest'],
    columns='scenario',
    values='tons'
)
```

---

## Performance Tips

1. **Use StateQuery when zone-level detail not needed** - Much faster
2. **Cache queries** - Repeated queries are instant
3. **Filter early** - Apply most selective filters first
4. **Use `estimate_size()`** - Check query size before execution
5. **Clear cache periodically** - If memory is constrained

---

## Error Handling

All query builders perform validation and provide helpful error messages:

```python
# Invalid year
query.years([2025])
# ValueError: Invalid years: [2025]. Valid years are 2017-2024...

# Invalid commodity
query.commodities(['InvalidCommodity'])
# ValueError: Could not find commodity 'InvalidCommodity'. Use faf.available_commodities()...

# Zone-level method on StateQuery
state_query.origin_zones([111])
# NotImplementedError: StateQuery does not support zone-level filtering...
```

---

## Version

Current version: **2.0.0**

```python
import tidyfaf as faf
print(faf.__version__)
# '2.0.0'
```
