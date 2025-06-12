-- Script: Initialize Snowflake Environment for Development
-- Author: M'hamed Issam ED-DAOU
-- Description:
--   This script sets up the necessary database, warehouse, and roles for both development
--   and business analysis purposes. It includes creation of environments and role-based
--   access management to ensure proper segmentation of duties and access control.

-- Use the Sysadmin role for administrative tasks
USE ROLE sysadmin;

-- Step 1: Create Database and Warehouse
-- Ensure the database and warehouse do not already exist before creating them
CREATE DATABASE IF NOT EXISTS DEV_DB_VISEO;
CREATE WAREHOUSE DEV_WH WITH WAREHOUSE_SIZE = 'X-SMALL';

-- Step 2: Create Schemas for Different Data Stages and drop the public schema
DROP SCHEMA IF EXISTS DEV_DB_VISEO.PUBLIC;
CREATE SCHEMA IF NOT EXISTS DEV_DB_VISEO.SAP_RAW_LANDING;
CREATE SCHEMA IF NOT EXISTS DEV_DB_VISEO.SAP_BRONZE;
CREATE SCHEMA IF NOT EXISTS DEV_DB_VISEO.SAP_SILVER;
CREATE SCHEMA IF NOT EXISTS DEV_DB_VISEO.SAP_GOLD;
CREATE SCHEMA IF NOT EXISTS DEV_DB_VISEO.UTILS;

-- Switch to Securityadmin role for role creation and management
USE ROLE securityadmin;

-- Step 3: Create Roles
CREATE ROLE DEV_VISEO;
CREATE ROLE CUSTOM_VISEO;

-- Step 4: Grant Roles to Sysadmin
-- Allows sysadmin to manage these roles without switching contexts
GRANT ROLE DEV_VISEO TO ROLE SYSADMIN;
GRANT ROLE CUSTOM_VISEO TO ROLE SYSADMIN;

-- Revert to Sysadmin role to set up grants
USE ROLE sysadmin;

-- Step 5: Display current grants on newly created database and warehouse (These steps can be used to troubleshoot or to monitor)
-- SHOW GRANTS ON DATABASE DEV_DB_VISEO;
-- SHOW GRANTS ON WAREHOUSE DEV_WH;

-- Step 6: Grant Necessary Privileges to Roles
-- Allow DEV_VISEO role to use the warehouse and access the database and schemas
GRANT USAGE ON WAREHOUSE DEV_WH TO ROLE DEV_VISEO;
GRANT ALL ON DATABASE DEV_DB_VISEO TO ROLE DEV_VISEO;
GRANT ALL ON SCHEMA DEV_DB_VISEO.SAP_BRONZE TO ROLE DEV_VISEO;
GRANT ALL ON SCHEMA DEV_DB_VISEO.SAP_SILVER TO ROLE DEV_VISEO;
GRANT ALL ON SCHEMA DEV_DB_VISEO.SAP_RAW_LANDING TO ROLE DEV_VISEO;

-- Role Specific Grants
-- Provide access to the SAP_GOLD schema to both DEV_VISEO and CUSTOM_VISEO roles
GRANT ALL ON SCHEMA DEV_DB_VISEO.SAP_GOLD TO ROLE DEV_VISEO;
GRANT ALL ON SCHEMA DEV_DB_VISEO.SAP_GOLD TO ROLE CUSTOM_VISEO;

-- Grant EXECUTE TASK privilege to roles
USE ROLE accountadmin;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DEV_VISEO;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CUSTOM_VISEO;


-- Step 7: Check Role Grants and User Overview (Troubleshoot // Monitor)
-- SHOW GRANTS TO ROLE DEV_VISEO;
-- SHOW USERS;

-- End of script
