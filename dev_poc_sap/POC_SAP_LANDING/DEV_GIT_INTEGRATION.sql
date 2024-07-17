-- Script: DEV_GIT_INTEGRATION.sql
-- Author: M'hamed Issam ED-DAOU

-- Create a secret named 'git_secret' for authentication
-- This secret uses a password type authentication with the specified username and personal access token
-- The username is the GitHub account email
-- The password is a GitHub personal access token with the necessary permissions
use role accountadmin;
CREATE OR REPLACE SECRET DEV_DB_VISEO.SAP_RAW_LANDING.git_secret
  TYPE = password
  USERNAME = 'eddaouissam@gmail.com'
  PASSWORD = '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';

-- Create an API integration named 'git_integration_poc' that integrates with a Git provider (GitHub in this case)
-- The API provider is specified as 'git_https_api' for HTTPS Git API access
-- The allowed prefixes specify which GitHub URL prefixes are permitted for this integration
-- Authentication secrets (git_secret) are allowed for this integration
-- The integration is enabled upon creation
use database DEV_DB_VISEO;
use schema SAP_RAW_LANDING;
CREATE OR REPLACE API INTEGRATION git_integration_poc
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/eddaouissam')
  ALLOWED_AUTHENTICATION_SECRETS = (git_secret)
  ENABLED = TRUE;

-- Create a Git repository reference named 'git_repo_stage' within the specified schema (DEV_DB_VISEO.SAP_RAW_LANDING)
-- The API integration used for this repository is 'git_integration_poc'
-- The Git credentials (authentication) used are specified by 'git_secret'
-- The origin specifies the URL of the GitHub repository to be integrated
CREATE OR REPLACE GIT REPOSITORY DEV_DB_VISEO.SAP_RAW_LANDING.git_repo_stage
  API_INTEGRATION = git_integration_poc
  GIT_CREDENTIALS = DEV_DB_VISEO.SAP_RAW_LANDING.git_secret
  ORIGIN = 'https://github.com/eddaouissam/poc_sap';

-- Grant the READ privilege on the created Git repository to the 'sysadmin' role
-- This allows users with the 'sysadmin' role to read the contents of the Git repository
grant READ on GIT REPOSITORY DEV_DB_VISEO.SAP_RAW_LANDING.git_repo_stage to role sysadmin;
