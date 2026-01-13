# Reporting & Business Intelligence

This directory contains resources and documentation for the reporting layer.

## Purpose

The reporting module provides:

- Interactive dashboards for business users
- KPI visualization
- Self-service analytics
- Report sharing capabilities

## Default Tool: Metabase

The framework uses **Metabase** as the default BI tool:

- **Open-source** and self-hosted
- **Containerized** deployment via Docker
- **User-friendly** interface for non-technical users
- **Direct integration** with PostgreSQL

## Accessing Metabase

Once the Docker Compose stack is running:

1. Navigate to `http://localhost:3000`
2. Complete the initial setup wizard
3. Connect to PostgreSQL:
   - **Database type**: PostgreSQL
   - **Host**: `postgres`
   - **Port**: `5432`
   - **Database name**: `gorigami_analytics`
   - **Username**: `gorigami`
   - **Password**: `gorigami_password`

## Best Practices

### Data Access

- Connect Metabase **only** to the `consumption` schema
- Never expose raw or curated schemas to business users
- Use database views to control data access

### Dashboard Design

1. **Start with questions** - What decisions will this dashboard support?
2. **Keep it simple** - Focus on key metrics
3. **Use filters** - Enable users to explore data
4. **Add context** - Include descriptions and definitions
5. **Optimize queries** - Use aggregated tables for performance

### Governance

- **Document dashboards** - Explain what metrics mean
- **Version control** - Export dashboard definitions
- **Control access** - Use Metabase permissions
- **Monitor usage** - Track which dashboards are used

## Resources

- [Metabase Documentation](https://www.metabase.com/docs/latest/)
- [Metabase Best Practices](https://www.metabase.com/learn/)
