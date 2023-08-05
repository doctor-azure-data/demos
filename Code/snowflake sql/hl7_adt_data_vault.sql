-- Script is for creating a data vault for ADT messages in HL7
-- Hubs
CREATE OR REPLACE TABLE hub_patient (
    patient_id STRING PRIMARY KEY
);

CREATE OR REPLACE TABLE hub_facility (
    facility_id STRING PRIMARY KEY
);

CREATE OR REPLACE TABLE hub_encounter (
    encounter_id STRING PRIMARY KEY
);

CREATE OR REPLACE TABLE hub_adt_event_type (
    adt_event_type_id STRING PRIMARY KEY
);

CREATE OR REPLACE TABLE hub_message (
    message_id STRING PRIMARY KEY
);

-- Links
CREATE OR REPLACE TABLE link_patient_facility (
    link_id STRING PRIMARY KEY,
    patient_id STRING,
    facility_id STRING
);

CREATE OR REPLACE TABLE link_encounter_patient (
    link_id STRING PRIMARY KEY,
    encounter_id STRING,
    patient_id STRING
);

CREATE OR REPLACE TABLE link_message_adt_event_type (
    link_id STRING PRIMARY KEY,
    message_id STRING,
    adt_event_type_id STRING
);

CREATE OR REPLACE TABLE link_message_encounter (
    link_id STRING PRIMARY KEY,
    message_id STRING,
    encounter_id STRING
);

-- Satellites
CREATE OR REPLACE TABLE sat_patient (
    patient_id STRING PRIMARY KEY,
    -- Add patient attributes
);

CREATE OR REPLACE TABLE sat_facility (
    facility_id STRING PRIMARY KEY,
    -- Add facility attributes
);

CREATE OR REPLACE TABLE sat_encounter (
    encounter_id STRING PRIMARY KEY,
    -- Add encounter attributes
);

CREATE OR REPLACE TABLE sat_adt_event_type (
    adt_event_type_id STRING PRIMARY KEY,
    -- Add ADT event type attributes
);

CREATE OR REPLACE TABLE sat_message (
    message_id STRING PRIMARY KEY,
    -- Add message attributes
);

