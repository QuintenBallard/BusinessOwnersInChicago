-- 1. BUSINESS OWNERS TABLE
CREATE TABLE IF NOT EXISTS public.chicago_business_owners (
    owner_id BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    account_number TEXT,
    legal_name TEXT,
    owner_first_name TEXT,
    owner_middle_initial TEXT,
    owner_last_name TEXT,
    suffix TEXT,
    legal_entity_owner TEXT,
    title TEXT,
    full_name TEXT
);


CREATE INDEX IF NOT EXISTS idx_business_owners_account_number
ON public.chicago_business_owners (account_number);

CREATE INDEX IF NOT EXISTS idx_business_owners_legal_name
ON public.chicago_business_owners (legal_name);

CREATE INDEX IF NOT EXISTS idx_business_owners_full_name
ON public.chicago_business_owners (full_name);


-- 2. BUSINESS LICENSES TABLE
CREATE TABLE IF NOT EXISTS public.chicago_business_licenses (
    id TEXT PRIMARY KEY,
    license_id INTEGER,
    account_number TEXT,
    site_number INTEGER,
    legal_name TEXT,
    doing_business_as_name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    zip_code TEXT,
    ward INTEGER,
    precinct INTEGER,
    ward_precinct TEXT,
    police_district INTEGER,
    community_area INTEGER,
    community_area_name TEXT,
    neighborhood TEXT,
    license_code TEXT,
    license_description TEXT,
    business_activity_id TEXT,
    business_activity TEXT,
    license_number TEXT,
    application_type TEXT,
    application_created_date DATE,
    application_requirements_complete DATE,
    payment_date DATE,
    conditional_approval TEXT,
    license_term_start_date DATE,
    license_term_expiration_date DATE,
    license_approved_for_issuance DATE,
    date_issued DATE,
    license_status TEXT,
    license_status_change_date DATE,
    ssa INTEGER,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    location TEXT
);


CREATE INDEX IF NOT EXISTS idx_business_licenses_account_number
ON public.chicago_business_licenses (account_number);

CREATE INDEX IF NOT EXISTS idx_business_licenses_license_id
ON public.chicago_business_licenses (license_id);

CREATE INDEX IF NOT EXISTS idx_business_licenses_legal_name
ON public.chicago_business_licenses (legal_name);


-- 3. MERGED CHICAGO BUSINESS TABLE
CREATE TABLE IF NOT EXISTS public.business_license_owners (
    chicago_business_id BIGINT
        GENERATED ALWAYS AS IDENTITY
        PRIMARY KEY,

    account_number TEXT,
    legal_name TEXT,
    community_area INTEGER,
    longitude DOUBLE PRECISION,
    ssa INTEGER,
    neighborhood TEXT,
    application_requirements_complete DATE,
    payment_date DATE,
    ward INTEGER,
    license_status TEXT,
    site_number INTEGER,
    community_area_name TEXT,
    location TEXT,
    address TEXT,
    latitude DOUBLE PRECISION,
    license_term_expiration_date DATE,
    id TEXT NOT NULL,
    police_district INTEGER,
    license_description TEXT,
    application_type TEXT,
    license_approved_for_issuance DATE,
    date_issued DATE,
    business_activity_id TEXT,
    city TEXT,
    doing_business_as_name TEXT,
    license_number TEXT,
    license_id INTEGER,
    state TEXT,
    license_status_change_date DATE,
    license_term_start_date DATE,
    license_code TEXT,
    zip_code TEXT,
    business_activity TEXT,
    precinct INTEGER,
    ward_precinct TEXT,
    conditional_approval TEXT,
    application_created_date DATE,
    legal_entity_owner TEXT,
    owner_first_name TEXT,
    title TEXT,
    suffix TEXT,
    full_name TEXT,
    owner_last_name TEXT,
    owner_middle_initial TEXT
);


CREATE INDEX IF NOT EXISTS idx_chicago_business_source_id
ON public.business_license_owners (id);

CREATE INDEX IF NOT EXISTS idx_chicago_business_account_number
ON public.business_license_owners (account_number);

CREATE INDEX IF NOT EXISTS idx_chicago_business_license_id
ON public.business_license_owners (license_id);

CREATE INDEX IF NOT EXISTS idx_chicago_business_legal_name
ON public.business_license_owners (legal_name);

CREATE INDEX IF NOT EXISTS idx_chicago_business_full_name
ON public.business_license_owners (full_name);