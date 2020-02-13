#!/usr/bin/env bash
# Company = Summit Live LLC.
# author  = luisfelipeyepezbarrios | date  =2/13/20
# project = syenapFTP, ticket_ftp

from pkg.utils import *

list_ticket_file = get_files(
    os.environ["URL_FTP_DOMAIN"],
    os.environ["URL_FTP_USERNAME"],
    os.environ["URL_FTP_PASSW"],
    os.environ["URL_FOLDER_T"],
)

ticket_csv_fr = chain_structure(list_ticket_file)

ticket_csv_reg = frame_reg(ticket_csv_fr, "ticket_reg")

if ticket_csv_reg is not None:
    ticket_dataframe = transform_structure(ticket_csv_reg)
    store_to_database(ticket_dataframe, "test_ticket")
else:
    print("Service Finish without Change")
