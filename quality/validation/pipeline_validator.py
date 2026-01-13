"""
Pipeline Validator
Validates data quality in pipelines with configurable rules.
"""

from typing import Dict, Any, List, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from datetime import datetime
from loguru import logger
import yaml


class ValidationRule:
    """
    Represents a single validation rule.
    """

    def __init__(
        self,
        name: str,
        rule_type: str,
        severity: str,
        config: Dict[str, Any]
    ):
        self.name = name
        self.rule_type = rule_type
        self.severity = severity  # error, warning, info
        self.config = config

    def __repr__(self) -> str:
        return f"ValidationRule(name={self.name}, type={self.rule_type}, severity={self.severity})"


class ValidationResult:
    """
    Result of a validation run.
    """

    def __init__(self):
        self.rules_executed = 0
        self.rules_passed = 0
        self.rules_failed = 0
        self.failures: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
        self.timestamp = datetime.now()

    @property
    def passed(self) -> bool:
        """Check if validation passed (no errors)"""
        return self.rules_failed == 0

    def add_failure(self, rule_name: str, severity: str, message: str, details: Optional[Dict] = None):
        """Add a validation failure"""
        failure = {
            'rule_name': rule_name,
            'severity': severity,
            'message': message,
            'details': details or {}
        }
        
        if severity == 'error':
            self.failures.append(failure)
            self.rules_failed += 1
        elif severity == 'warning':
            self.warnings.append(failure)
        
        self.rules_executed += 1

    def add_success(self):
        """Add a successful validation"""
        self.rules_passed += 1
        self.rules_executed += 1

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'passed': self.passed,
            'rules_executed': self.rules_executed,
            'rules_passed': self.rules_passed,
            'rules_failed': self.rules_failed,
            'failures': self.failures,
            'warnings': self.warnings
        }

    def __repr__(self) -> str:
        status = "PASSED" if self.passed else "FAILED"
        return f"ValidationResult({status}, {self.rules_passed}/{self.rules_executed} passed)"


class PipelineValidator:
    """
    Validates data quality in pipelines using configurable rules.
    
    Supports validation of:
    - Schema (required columns, data types)
    - Null values
    - Business rules
    - Freshness
    - Volume
    - Referential integrity
    """

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
        rules_path: Optional[str] = None,
        rules: Optional[List[Dict[str, Any]]] = None
    ):
        self.spark = spark
        self.rules: List[ValidationRule] = []
        
        if rules_path:
            self._load_rules_from_yaml(rules_path)
        elif rules:
            self._load_rules_from_dict(rules)

    def _load_rules_from_yaml(self, rules_path: str) -> None:
        """Load validation rules from YAML file"""
        with open(rules_path, 'r') as f:
            config = yaml.safe_load(f)
        
        self._load_rules_from_dict(config.get('rules', []))
        logger.info(f"Loaded {len(self.rules)} rules from {rules_path}")

    def _load_rules_from_dict(self, rules_config: List[Dict[str, Any]]) -> None:
        """Load rules from dictionary"""
        for rule_config in rules_config:
            rule = ValidationRule(
                name=rule_config['name'],
                rule_type=rule_config['type'],
                severity=rule_config.get('severity', 'error'),
                config=rule_config.get('config', {})
            )
            self.rules.append(rule)

    def validate_dataframe(self, df: DataFrame, layer: str = 'unknown') -> ValidationResult:
        """
        Validate a Spark DataFrame.

        Args:
            df: DataFrame to validate
            layer: Data layer (bronze, silver, gold)

        Returns:
            ValidationResult
        """
        logger.info(f"Validating {layer} layer with {len(self.rules)} rules")
        
        result = ValidationResult()
        
        for rule in self.rules:
            try:
                self._execute_rule(rule, df, result)
            except Exception as e:
                logger.error(f"Error executing rule '{rule.name}': {e}")
                result.add_failure(
                    rule_name=rule.name,
                    severity='error',
                    message=f"Rule execution failed: {e}"
                )
        
        logger.info(f"Validation result: {result}")
        return result

    def _execute_rule(self, rule: ValidationRule, df: DataFrame, result: ValidationResult) -> None:
        """Execute a single validation rule"""
        if rule.rule_type == 'schema':
            self._validate_schema(rule, df, result)
        elif rule.rule_type == 'null_check':
            self._validate_nulls(rule, df, result)
        elif rule.rule_type == 'business_rule':
            self._validate_business_rule(rule, df, result)
        elif rule.rule_type == 'freshness':
            self._validate_freshness(rule, df, result)
        elif rule.rule_type == 'volume':
            self._validate_volume(rule, df, result)
        elif rule.rule_type == 'uniqueness':
            self._validate_uniqueness(rule, df, result)
        else:
            logger.warning(f"Unknown rule type: {rule.rule_type}")
            result.add_success()

    def _validate_schema(self, rule: ValidationRule, df: DataFrame, result: ValidationResult) -> None:
        """Validate schema (required columns)"""
        required_columns = rule.config.get('columns', [])
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            result.add_failure(
                rule_name=rule.name,
                severity=rule.severity,
                message=f"Missing required columns: {missing_columns}",
                details={'missing_columns': list(missing_columns)}
            )
        else:
            result.add_success()

    def _validate_nulls(self, rule: ValidationRule, df: DataFrame, result: ValidationResult) -> None:
        """Validate null values"""
        columns = rule.config.get('columns', [])
        max_null_percentage = rule.config.get('max_null_percentage', 0)
        
        total_rows = df.count()
        
        for col in columns:
            if col not in df.columns:
                result.add_failure(
                    rule_name=rule.name,
                    severity=rule.severity,
                    message=f"Column '{col}' not found"
                )
                continue
            
            null_count = df.filter(F.col(col).isNull()).count()
            null_percentage = (null_count / total_rows * 100) if total_rows > 0 else 0
            
            if null_percentage > max_null_percentage:
                result.add_failure(
                    rule_name=rule.name,
                    severity=rule.severity,
                    message=f"Column '{col}' has {null_percentage:.2f}% nulls (max: {max_null_percentage}%)",
                    details={'column': col, 'null_percentage': null_percentage}
                )
            else:
                result.add_success()

    def _validate_business_rule(self, rule: ValidationRule, df: DataFrame, result: ValidationResult) -> None:
        """Validate business rule (SQL condition)"""
        condition = rule.config.get('condition')
        
        if not condition:
            result.add_success()
            return
        
        # Count violations
        violations = df.filter(f"NOT ({condition})").count()
        
        if violations > 0:
            result.add_failure(
                rule_name=rule.name,
                severity=rule.severity,
                message=f"Business rule violated: {violations} records don't satisfy '{condition}'",
                details={'violations': violations, 'condition': condition}
            )
        else:
            result.add_success()

    def _validate_freshness(self, rule: ValidationRule, df: DataFrame, result: ValidationResult) -> None:
        """Validate data freshness"""
        column = rule.config.get('column')
        max_age_hours = rule.config.get('max_age_hours', 24)
        
        if column not in df.columns:
            result.add_failure(
                rule_name=rule.name,
                severity=rule.severity,
                message=f"Freshness column '{column}' not found"
            )
            return
        
        # Get max timestamp
        max_timestamp = df.agg(F.max(column)).collect()[0][0]
        
        if max_timestamp:
            age_hours = (datetime.now() - max_timestamp).total_seconds() / 3600
            
            if age_hours > max_age_hours:
                result.add_failure(
                    rule_name=rule.name,
                    severity=rule.severity,
                    message=f"Data is {age_hours:.1f} hours old (max: {max_age_hours} hours)",
                    details={'age_hours': age_hours, 'max_timestamp': str(max_timestamp)}
                )
            else:
                result.add_success()
        else:
            result.add_success()

    def _validate_volume(self, rule: ValidationRule, df: DataFrame, result: ValidationResult) -> None:
        """Validate data volume"""
        min_rows = rule.config.get('min_rows', 0)
        max_rows = rule.config.get('max_rows', float('inf'))
        
        row_count = df.count()
        
        if row_count < min_rows:
            result.add_failure(
                rule_name=rule.name,
                severity=rule.severity,
                message=f"Row count {row_count} below minimum {min_rows}",
                details={'row_count': row_count, 'min_rows': min_rows}
            )
        elif row_count > max_rows:
            result.add_failure(
                rule_name=rule.name,
                severity=rule.severity,
                message=f"Row count {row_count} above maximum {max_rows}",
                details={'row_count': row_count, 'max_rows': max_rows}
            )
        else:
            result.add_success()

    def _validate_uniqueness(self, rule: ValidationRule, df: DataFrame, result: ValidationResult) -> None:
        """Validate uniqueness of key columns"""
        columns = rule.config.get('columns', [])
        
        total_rows = df.count()
        distinct_rows = df.select(columns).distinct().count()
        
        if total_rows != distinct_rows:
            duplicates = total_rows - distinct_rows
            result.add_failure(
                rule_name=rule.name,
                severity=rule.severity,
                message=f"Found {duplicates} duplicate records on {columns}",
                details={'duplicates': duplicates, 'columns': columns}
            )
        else:
            result.add_success()


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("Validator").getOrCreate()
    
    # Create validator with rules
    validator = PipelineValidator(
        spark=spark,
        rules_path='config/quality/bronze_rules.yaml'
    )
    
    # Read data
    df = spark.read.parquet('/data/bronze/sales/*.parquet')
    
    # Validate
    result = validator.validate_dataframe(df, layer='bronze')
    
    print(f"Validation: {result}")
    print(f"Passed: {result.passed}")
    print(f"Failures: {result.failures}")
    
    spark.stop()
