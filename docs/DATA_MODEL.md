# Synthetic source data model

- `patients`: `patient_id` primary key; fabricated date of birth, sex and state code.
- `providers`: `provider_id` primary key and specialty.
- `encounters`: `encounter_id` primary key, `patient_id` and `provider_id` foreign keys, date and encounter type.
- `claims`: `claim_id` primary key, `encounter_id` foreign key, billed amount in **integer cents**, status and service date.

For education, a PAID claim contributes its full billed amount and a DENIED claim contributes zero. Real adjudication/reimbursement is much more complex. All data is fabricated and not medically or financially representative.
