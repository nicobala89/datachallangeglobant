"""
Source Profiler
Profiles source data for exploratory analysis and audit purposes.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from loguru import logger
import json


class SourceProfile:
    """
    Container for source profiling results.
    """

    def __init__(self, source_name: str, profile_data: Dict[str, Any]):
        self.source_name = source_name
        self.profile_data = profile_data
        self.timestamp = datetime.now()

    def to_dict(self) -> Dict[str, Any]:
        """Convert profile to dictionary"""
        return {
            'source_name': self.source_name,
            'timestamp': self.timestamp.isoformat(),
            'profile': self.profile_data
        }

    def to_json(self, output_path: str) -> None:
        """Save profile as JSON"""
        with open(output_path, 'w') as f:
            json.dump(self.to_dict(), f, indent=2)
        logger.info(f"Saved profile to {output_path}")

    def __repr__(self) -> str:
        return f"SourceProfile(source={self.source_name}, columns={len(self.profile_data.get('columns', []))})"


class SourceProfiler:
    """
    Profiles source data files and tables.
    
    Generates statistics for:
    - Column data types
    - Null counts and percentages
    - Distinct value counts
    - Min/max/mean/median/std
    - Sample values
    - Data quality score
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.sample_size = self.config.get('sample_size', 10)

    def profile_file(self, file_path: str) -> SourceProfile:
        """
        Profile a data file (CSV, Parquet, Excel).

        Args:
            file_path: Path to file

        Returns:
            SourceProfile with statistics
        """
        logger.info(f"Profiling file: {file_path}")
        
        # Read file
        df = self._read_file(file_path)
        
        # Generate profile
        profile_data = self._profile_dataframe(df)
        
        # Add file metadata
        profile_data['file_info'] = self._get_file_info(file_path)
        
        return SourceProfile(
            source_name=Path(file_path).name,
            profile_data=profile_data
        )

    def profile_dataframe(self, df: pd.DataFrame, source_name: str) -> SourceProfile:
        """
        Profile a pandas DataFrame.

        Args:
            df: DataFrame to profile
            source_name: Name for the source

        Returns:
            SourceProfile with statistics
        """
        logger.info(f"Profiling DataFrame: {source_name}")
        
        profile_data = self._profile_dataframe(df)
        
        return SourceProfile(
            source_name=source_name,
            profile_data=profile_data
        )

    def _read_file(self, file_path: str) -> pd.DataFrame:
        """Read file into DataFrame"""
        path = Path(file_path)
        suffix = path.suffix.lower()
        
        if suffix == '.csv':
            return pd.read_csv(file_path)
        elif suffix == '.parquet':
            return pd.read_parquet(file_path)
        elif suffix in ['.xlsx', '.xls']:
            return pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format: {suffix}")

    def _profile_dataframe(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Generate comprehensive profile of DataFrame"""
        profile = {
            'overview': self._get_overview(df),
            'columns': self._profile_columns(df),
            'quality_score': self._calculate_quality_score(df)
        }
        
        return profile

    def _get_overview(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get overview statistics"""
        return {
            'total_rows': len(df),
            'total_columns': len(df.columns),
            'memory_usage_mb': round(df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            'duplicate_rows': int(df.duplicated().sum()),
            'duplicate_percentage': round(df.duplicated().sum() / len(df) * 100, 2) if len(df) > 0 else 0
        }

    def _profile_columns(self, df: pd.DataFrame) -> List[Dict[str, Any]]:
        """Profile each column"""
        columns_profile = []
        
        for col in df.columns:
            col_profile = {
                'name': col,
                'dtype': str(df[col].dtype),
                'null_count': int(df[col].isnull().sum()),
                'null_percentage': round(df[col].isnull().sum() / len(df) * 100, 2) if len(df) > 0 else 0,
                'distinct_count': int(df[col].nunique()),
                'distinct_percentage': round(df[col].nunique() / len(df) * 100, 2) if len(df) > 0 else 0,
            }
            
            # Add numeric statistics
            if pd.api.types.is_numeric_dtype(df[col]):
                col_profile.update(self._get_numeric_stats(df[col]))
            
            # Add string statistics
            elif pd.api.types.is_string_dtype(df[col]) or df[col].dtype == 'object':
                col_profile.update(self._get_string_stats(df[col]))
            
            # Add datetime statistics
            elif pd.api.types.is_datetime64_any_dtype(df[col]):
                col_profile.update(self._get_datetime_stats(df[col]))
            
            # Add sample values
            col_profile['sample_values'] = df[col].dropna().head(self.sample_size).tolist()
            
            columns_profile.append(col_profile)
        
        return columns_profile

    def _get_numeric_stats(self, series: pd.Series) -> Dict[str, Any]:
        """Get statistics for numeric columns"""
        return {
            'min': float(series.min()) if not series.isnull().all() else None,
            'max': float(series.max()) if not series.isnull().all() else None,
            'mean': round(float(series.mean()), 2) if not series.isnull().all() else None,
            'median': float(series.median()) if not series.isnull().all() else None,
            'std': round(float(series.std()), 2) if not series.isnull().all() else None,
            'zeros_count': int((series == 0).sum()),
            'negative_count': int((series < 0).sum()) if not series.isnull().all() else 0
        }

    def _get_string_stats(self, series: pd.Series) -> Dict[str, Any]:
        """Get statistics for string columns"""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {
                'min_length': None,
                'max_length': None,
                'avg_length': None,
                'empty_count': 0
            }
        
        lengths = non_null.astype(str).str.len()
        
        return {
            'min_length': int(lengths.min()),
            'max_length': int(lengths.max()),
            'avg_length': round(float(lengths.mean()), 2),
            'empty_count': int((non_null.astype(str).str.strip() == '').sum())
        }

    def _get_datetime_stats(self, series: pd.Series) -> Dict[str, Any]:
        """Get statistics for datetime columns"""
        non_null = series.dropna()
        
        if len(non_null) == 0:
            return {'min_date': None, 'max_date': None}
        
        return {
            'min_date': str(non_null.min()),
            'max_date': str(non_null.max()),
            'date_range_days': (non_null.max() - non_null.min()).days if len(non_null) > 0 else 0
        }

    def _calculate_quality_score(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate overall data quality score"""
        total_cells = len(df) * len(df.columns)
        
        if total_cells == 0:
            return {'overall_score': 0, 'completeness': 0, 'uniqueness': 0}
        
        # Completeness: percentage of non-null values
        null_cells = df.isnull().sum().sum()
        completeness = round((1 - null_cells / total_cells) * 100, 2)
        
        # Uniqueness: average distinct percentage across columns
        uniqueness = round(df.nunique().mean() / len(df) * 100, 2) if len(df) > 0 else 0
        
        # Overall score (weighted average)
        overall_score = round((completeness * 0.7 + uniqueness * 0.3), 2)
        
        return {
            'overall_score': overall_score,
            'completeness': completeness,
            'uniqueness': uniqueness
        }

    def _get_file_info(self, file_path: str) -> Dict[str, Any]:
        """Get file metadata"""
        path = Path(file_path)
        stat = path.stat()
        
        return {
            'file_name': path.name,
            'file_path': str(path.absolute()),
            'file_size_mb': round(stat.st_size / 1024 / 1024, 2),
            'modified_at': datetime.fromtimestamp(stat.st_mtime).isoformat()
        }


# Example usage
if __name__ == "__main__":
    profiler = SourceProfiler()
    
    # Profile a file
    profile = profiler.profile_file('/data/source/sales.csv')
    
    print(f"Profile: {profile}")
    print(f"Total rows: {profile.profile_data['overview']['total_rows']}")
    print(f"Quality score: {profile.profile_data['quality_score']['overall_score']}")
    
    # Save as JSON
    profile.to_json('/reports/sales_profile.json')
