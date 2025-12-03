# tidyfaf

**tidyfaf** is a user-friendly Python package for accessing FAF5 (Freight Analysis Framework) freight flow data. Inspired by `tidycensus`, it provides chainable query builders for exploring origin-destination flows, commodities, modes, and highway networks.

## Features

- 🔗 **Chainable queries**: Build complex filters with intuitive method chaining
- 🚀 **Lazy loading**: Data loads only when needed, with smart caching
- 🔍 **Discoverable**: Built-in search functions eliminate need for documentation lookups
- 📊 **Analysis-ready**: Pre-built aggregation and summary methods
- 🗺️ **Geometry support**: Easy conversion to GeoDataFrames for mapping
- 🎯 **Multi-level geography**: Support for both state and zone-level queries

## Installation

```bash
pip install tidyfaf
```

## Quick Start

### Discovery

Explore available data without reading documentation:

```python
import tidyfaf as faf

# Search for commodities
faf.available_commodities(search='electronics')

# Search for zones
faf.available_zones(search='california')

# List all modes
faf.available_modes()
```

### Basic Flow Query

```python
# Query regional flows with method chaining
query = (faf.FAFQuery()
    .origin_states(['California', 'Texas'])
    .destination_zones([111])  # Washington DC area
    .commodities(['Electronics', 'Pharmaceuticals'])
    .years([2020, 2030])
)

# Get data as DataFrame (wide format)
df = query.get()

# Or get as tidy/long format
df_long = query.get(format='long')
```

### Cross-Level Queries

Query at different geographic levels for origin and destination:

```python
# Origin: States, Destination: Specific zones
query = (faf.FAFQuery()
    .origin_states(['California', 'Texas', 'New York'])
    .destination_zones([111, 121, 131])  # Specific metro areas
    .commodities(['Electronics'])
    .years([2020])
)

df = query.get()
```

### Built-in Analysis

```python
query = (faf.FAFQuery()
    .origin_states(['California'])
    .commodities(['Electronics', 'Pharmaceuticals'])
    .years([2020])
)

# Group by destination
by_dest = query.by_destination(metrics=['tons', 'value'])

# Group by commodity
by_commodity = query.by_commodity()

# Top N flows
top_flows = query.top(n=10, by='tons', year=2020)

# Summary statistics
stats = query.summarize(metric='tons', year=2020)
print(stats)
# {'total': 1234567, 'mean': 45.6, 'median': 23.4, 'flows': 1500}
```

### Multi-Year Analysis

```python
# Compare multiple years
query = (faf.FAFQuery()
    .origin_states(['California'])
    .destination_states(['Texas'])
    .commodities(['Electronics'])
    .year_range(2017, 2024)
)

# Wide format: tons_2017, tons_2020, tons_2024, tons_2030 columns
df_wide = query.get(format='wide')

# Long/tidy format: year as dimension
df_long = query.get(format='long')
```

### Geometry and Mapping

```python
# Get flows with LineString geometries
gdf = query.to_gdf()

# Create interactive map
from tidyfaf.visualization import FlowMap
FlowMap(gdf).generate_map('flows.html', flow_column='tons_2020')
```

## Query Types

### FAFQuery - Regional Zone-Level Flows

Most detailed level - 132 FAF zones across the US.

```python
query = (faf.FAFQuery()
    .origin_zones([61, 62])  # Specific zones in California
    .destination_zones([111, 112])  # DC and Baltimore areas
    .commodities(['Electronics'])
    .modes(['Truck', 'Rail'])
    .years([2020, 2030])
)
```

**Available methods**:
- `.origin_states(list)` - Filter by origin states
- `.destination_states(list)` - Filter by destination states
- `.origin_zones(list)` - Filter by origin FAF zones
- `.destination_zones(list)` - Filter by destination zones
- `.commodities(list)` - Filter by commodity
- `.modes(list)` - Filter by mode (Truck, Rail, Water, Air, etc.)
- `.years(list)` - Select specific years
- `.year_range(start, end)` - Select year range
- `.trade_types(list)` - Domestic, Import, Export
- `.min_tons(value, year)` - Threshold filter
- `.by_origin()`, `.by_destination()`, `.by_commodity()` - Aggregations
- `.top(n, by, year)` - Top N flows
- `.summarize()` - Quick stats

### StateQuery - State-Level Flows

Faster queries for state-level analysis.

```python
query = (faf.StateQuery()
    .origin_states(['California', 'Texas'])
    .destination_states(['Washington', 'Oregon'])
    .commodities(['Electronics'])
    .years([2020, 2025, 2030])
)

df = query.get()
```

**Note**: StateQuery does not support zone-level filtering or geometry conversion.

### NetworkQuery - Highway Network

Analyze FAF5 highway network.

```python
network = (faf.NetworkQuery()
    .routes(['I-5', 'I-95', 'US-101'])
    .states(['CA', 'OR', 'WA'])
    .freight_network(True)  # National Highway Freight Network only
    .truck_allowed(True)  # Exclude prohibited segments
)

gdf = network.get()
print(f"Total length: {network.total_length():,.0f} miles")
```

**Available methods**:
- `.routes(list)` - Filter by route number
- `.states(list)` - Filter by state
- `.zones(list)` - Filter by FAF zone
- `.functional_classes(list)` - Interstate, Arterial, etc.
- `.freight_network(bool)` - NHFN segments
- `.nhs(bool)` - National Highway System
- `.truck_allowed(bool)` - Truck access
- `.toll_roads(bool)` - Toll status
- `.total_length()` - Sum of link lengths
- `.by_state()` - Group by state

### ForecastQuery - Scenario Analysis

Analyze base/high/low forecast scenarios.

```python
forecast = (faf.ForecastQuery()
    .origin_states(['California'])
    .destination_states(['Texas'])
    .commodities(['Electronics'])
    .years([2030, 2040, 2050])
    .scenarios(['base', 'high', 'low'])
)

# Returns data with 'scenario' column
df = forecast.get(format='long')

# Compare scenarios for specific year
comparison = forecast.compare_scenarios(year=2030)
```

## Advanced Features

### Immutable Queries

Queries are immutable - each filter returns a new instance:

```python
base = faf.FAFQuery().origin_states(['California'])

electronics = base.commodities(['Electronics'])
pharma = base.commodities(['Pharmaceuticals'])

# Different results - base query unchanged
df1 = electronics.get()  # Slower
df2 = pharma.get()  # Faster!
```

### Caching

Results are automatically cached for performance:

```python
query = faf.FAFQuery().origin_states(['California']).commodities(['Electronics'])

# First call loads data
df1 = query.get()  # Slower

# Second call uses cache
df2 = query.get()  # Faster!

# Clear cache if needed
faf.clear_cache()
```

### Custom Aggregations

```python
query = faf.FAFQuery().origin_states(['California'])

# Custom grouping
custom = query.group_by(
    fields=['dms_orig', 'sctg2', 'dms_mode'],
    metrics=['tons', 'value'],
    years=[2020]
)
```

## Data Setup and Management

The `tidyfaf` package expects FAF5 data files to be located in `~/.tidyfaf_data/` by default. This ensures data is stored separately from the package installation and is accessible across sessions.

### Automatic Initial Download

Upon the first import of `tidyfaf` (e.g., `import tidyfaf`), the package will check if the core FAF data (`FAF5_metadata.xlsx` and main regional/state flow files) are present in `~/.tidyfaf_data/`. If they are missing, the package will attempt to automatically download and process these files from their official sources (faf.ornl.gov, bts.gov).

This process can take several minutes depending on your internet connection and will print status updates to the console.

### Handling County Factors (Manual Setup Option)

While the core FAF data is downloaded automatically, the download of **County-Level Disaggregation Factors** (`All_Experimental_Disaggregation_Factors.zip`) can sometimes be unreliable due to strict server-side restrictions on government websites, leading to `ConnectionResetError` or `HTTP 403 Forbidden` errors.

If you encounter issues with the automatic download of county factors, you can perform a manual setup:

1.  **Manually Download the Zip File:**
    *   Visit the official source: `https://faf.ornl.gov/faf5/Data/County/All_Experimental_Disaggregation_Factors.zip`
    *   Download this zip file to your local machine.

2.  **Use `tidyfaf.setup_county_data()`:**
    Once downloaded, use the provided utility function to process the data into the correct location:
    ```python
    import tidyfaf as faf
    from pathlib import Path

    # Assuming the downloaded zip file is in your current working directory
    # or provide the full path to where you saved it.
    zip_file_path = Path("./All_Experimental_Disaggregation_Factors.zip") 

    if zip_file_path.exists():
        faf.setup_county_data(zip_file_path)
        print("County factor data successfully set up.")
    else:
        print(f"Error: {zip_file_path} not found. Please ensure the file is downloaded.")
    ```
    This function will extract the zip file, convert the contained CSV factors into optimized Parquet format, and store them in `~/.tidyfaf_data/county_factors/`, making them ready for use with `tidyfaf.CountyQuery`.

### Clearing Cached Data

For development or to force a fresh download, you can clear the package's internal data caches:

```python
import tidyfaf as faf

# Clear only query results cache (keeps raw data)
faf.clear_cache()

# Clear all caches, including raw downloaded data files
# This will force a re-download of core FAF data on next import/query.
faf.clear_all_caches()
```

## Examples

### Example 1: Top Origin-Destination Pairs

```python
import tidyfaf as faf

# Find top OD pairs for electronics from California
query = (faf.FAFQuery()
    .origin_states(['California'])
    .commodities(['Electronics'])
    .years([2020])
)

top_flows = query.top(10, by='tons', year=2020)
print(top_flows[['dms_orig', 'dms_dest', 'tons_2020']])
```

### Example 2: Commodity Comparison

```python
# Compare different commodities
query = (faf.FAFQuery()
    .origin_states(['California'])
    .destination_states(['Texas'])
    .commodities(['Electronics', 'Pharmaceuticals', 'Machinery'])
    .years([2020])
)

by_commodity = query.by_commodity(metrics=['tons'], years=[2020])
print(by_commodity.sort_values('tons_2020', ascending=False))
```

### Example 3: Year-over-Year Growth

```python
# Analyze growth from 2017 to 2024
query = (faf.FAFQuery()
    .origin_states(['California'])
    .commodities(['Electronics'])
    .year_range(2017, 2024)
)

df = query.get(format='long')

# Calculate growth
growth = df.groupby('year')['tons'].sum()
print(growth.pct_change())
```

### Example 4: I-5 Corridor Analysis

```python
# Analyze I-5 freight corridor
network = (faf.NetworkQuery()
    .routes(['I-5'])
    .states(['CA', 'OR', 'WA'])
    .freight_network(True)
)

gdf = network.get()
by_state = network.by_state()
print(f"Total I-5 NHFN miles: {network.total_length():,.0f}")
```

## API Reference

See [documentation](https://tidyfaf.readthedocs.io) for complete API reference.

## Data Sources

- **FAF5.7.1** (FHWA/BTS) - Regional and state-level freight flows
- **FAF5 Network** (FHWA) - Highway network with freight designations
- **FAF5 HiLo Forecasts** - Base/high/low growth scenarios
- **FAF5 County Factors** (FHWA/BTS) - Experimental county-level disaggregation factors

## Contributing

Contributions welcome! Please open an issue or submit a pull request.

## License

MIT License

## Citation

If you use this package in research, please cite:

```
@software{tidyfaf2025,
  title = {tidyfaf: Tidy access to FAF freight flow data},
  year = {2025},
  url = {https://github.com/yourusername/tidyfaf}
}
```