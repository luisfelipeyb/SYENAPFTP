#!/usr/bin/env bash
# Company = Summit Live LLC.
# author  = luisfelipeyepezbarrios | date  =2/13/20
# project = syenapFTP, cross_sale_ftp

from pkg.utils import *

list_sales_file = get_files(
    os.environ["URL_FTP_DOMAIN"],
    os.environ["URL_FTP_USERNAME"],
    os.environ["URL_FTP_PASSW"],
    os.environ["URL_FOLDER_V"],
)

sales_csv_fr = chain_structure(list_sales_file)

sales_csv_reg = frame_reg(sales_csv_fr, "cross_sale")

if sales_csv_reg is not None:
    cross_sales_dataframe = cross_sale_transform(sales_csv_reg)
    store_to_database(cross_sales_dataframe, "cross_sale")
else:
    print("Service Finish without Change")
