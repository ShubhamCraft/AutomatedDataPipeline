# AutomatedDataPipeline

## Overview

This project automates the process of downloading, processing, and transferring data from PriceLabs to a remote database. It includes functionalities for:

1. **Downloading Report Files:** The script automates logging into the PriceLabs website and downloading report files for specified market groups.
2. **Concatenating Data:** It concatenates the downloaded CSV files into a single DataFrame.
3. **Transferring Data:** The concatenated data is transferred from a local database to a remote database.
4. **Email Notifications:** An email notification is sent upon successful data transfer.
