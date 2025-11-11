# 🔐 Azure Active Directory (AAD) & Unity Catalog RBAC Setup

## 1️⃣ Enable Unity Catalog for Your Workspace
1. In the **Databricks Admin Console**, go to **Data Governance → Unity Catalog**.
2. Create a **Metastore** (if not already).
3. Assign the Metastore to your workspace.

## 2️⃣ Configure Azure Active Directory Integration
1. Connect your Databricks workspace to **Azure AD**.
2. Use **SCIM (System for Cross-domain Identity Management)** to sync users & groups.
3. Ensure that:
   - `call_admins` group → Full permissions
   - `call_reviewers` group → Read-only permissions

## 3️⃣ Enable Credential Passthrough
This ensures users access storage with their AAD identity.

- For interactive clusters:
  - Enable *Credential Passthrough* in the cluster settings.
- For jobs:
  - Use a **Service Principal** with restricted role-based credentials.

## 4️⃣ RBAC Enforcement in Unity Catalog
Examples:

```sql
GRANT USAGE ON CATALOG call_intel_catalog TO `call_reviewers`;
GRANT SELECT ON SCHEMA production TO `call_reviewers`;
GRANT MODIFY, SELECT ON SCHEMA production TO `call_admins`;
