# Spatial Ensemble Framework for Weather Forecast Bias Detection

## Abstract

This repository presents a novel methodology for detecting systematic forecast errors in weather predictions using NOAA gridpoint triangulation. The framework employs spatial consensus analysis across multiple observation points to identify and quantify forecast biases.

## Architecture

The system implements a multi-dimensional approach to weather forecast validation:

- **Multi-City Collection**: Simultaneous data collection across multiple metropolitan areas using NOAA weather grid infrastructure
- **Spatial Triangulation**: Each target location is evaluated using multiple nearby NOAA gridpoints (typically 29 grids per airport location)
- **Temporal Deduplication**: Robust handling of forecast updates and retractions to ensure data integrity
- **Spatial Consensus Metrics**: Aggregation algorithms that leverage geographic redundancy to detect systematic biases

## Methodology Overview

The framework collects forecast data from commercial weather services and compares predictions against authoritative observation networks. By triangulating forecasts across multiple gridpoints and analyzing patterns over time, the system can identify persistent biases in temperature predictions.

Key features:
- Automated forecast collection with temporal tracking
- Spatial ensemble analysis across gridpoint clusters
- Bias detection through statistical aggregation
- Support for multiple metropolitan areas simultaneously

## Technical Stack

- **Database**: SQLite with optimized schema for temporal and spatial queries
- **Data Sources**: NOAA Weather API, Weather Underground historical data
- **Analysis**: Python-based statistical frameworks

## Repository Contents

- `schema.sql` - Database schema for forecasts and actuals
- `scrape_template.py` - Template for web scraping weather forecasts
- `analyze_template.py` - Framework for bias calculation and analysis
- `requirements.txt` - Python dependencies
- `.gitignore` - Standard exclusions for data and logs

## Usage

This repository provides the foundational framework and methodology. Specific implementation details for selectors and scraping logic will need to be customized based on target weather services and their current web interfaces.

See the database schema for the expected data structure and the analysis template for the statistical approach to bias detection.

## Research Status

Full results from this methodology will be published in a forthcoming paper (February 2026). This repository serves as a reference implementation of the spatial ensemble framework described in that work.

## License

MIT License - See LICENSE file for details

## Citation

If you use this framework in your research, please cite the forthcoming paper:

```
[Citation details to be added upon publication - February 2026]
```

## Contact

For questions about the methodology or implementation, please open an issue in this repository.

