# sap_to_vendor_portal

Extract SD Order data from SAP, map to the Vendor Portal fields
https://docs.google.com/spreadsheets/d/18r3Q1FZeU4SiIt4v3xzZPriZaHYaQ0I68YPOd-fSu9Q


## How to use?

Copy config_template.json to config.json, set username and password for the HANA DB.

Put order_ids to the file "order_list.csv" located at the same folder as a script. One order_id per line.

Execute the script. Find the result in the folder "output" in JSON format, one file per oder.