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
    .years([2017, 2020, 2024, 2030])
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
print(f"Total length: {network.total_length()} miles")
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
df1 = electronics.get()
df2 = pharma.get()
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

## Data Setup

First-time setup downloads FAF5 data (~2GB):

```python
import tidyfaf as faf
faf.download_and_process()
```

Data is stored in `~/.tidyfaf_data/` by default.

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
  url = {https://github.com/darshanpandit/tidyfaf}
}
```
