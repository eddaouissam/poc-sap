-- Script: Create and Configure User for Development Environment
-- Author: M'hamed Issam ED-DAOU
-- Description:
--   This script creates a new user for the development environment in Snowflake
--   and assigns the necessary role. It is intended for initial setup of new developers.
--   Passwords must be changed upon first login for security compliance.

-- Step 1: Create the User
-- Create a user with a temporary password and set conditions for the account
CREATE USER DEV_USER_VISEO
  PASSWORD = 'Temp123'  -- Temporary password, must be changed on first login
  DEFAULT_ROLE = DEV_VISEO  -- Setting default role for the user
  MUST_CHANGE_PASSWORD = TRUE;  -- Enforces password change on first login for security

-- Step 2: Grant the Role
-- Assign the DEV_VISEO role to the new user
GRANT ROLE DEV_VISEO TO USER DEV_USER_VISEO;

-- End of script
