import pandas as pd
import datetime as dt
from dateutil.parser import parse
from ConfigParser import SafeConfigParser
import os
import sys
import logging
import xlrd
import math



def read_variable(section):
    path = parser.get(section, 'path')
    delimiter = parser.get(section, 'delimiter')
    skipheader = parser.get(section, 'skipheader')
    return path.strip(), delimiter.strip(), skipheader.strip()


# check if input is a date
def is_date(string):
    try:
        parse(string)
        return True
    except ValueError:
        return False


# check if input is a number
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


# Will format the date to a particular format
def format_date(date_string):
    fmts = ('%b %d, %Y', '%b %d, %Y', '%B %d, %Y', '%B %d %Y', '%m/%d/%Y', '%m/%d/%y', '%b %d,%Y'
            , '%Y/%m/%d', '%Y-%m-%d', '%d-%b-%Y', '%Y%m%d', '%H:%M:%S', '%H.%M.%S')

    lst = ["20", "19"]

    for fmt in fmts:
        try:
            if fmt == '%Y%m%d':
                if len(date_string) != 8 or date_string[:2] not in lst:
                    raise ValueError
            t = dt.datetime.strptime(date_string, fmt)
            return str(t)
            break
        except ValueError as err:
            pass

    return date_string


# will format the numbers
def format_number(data):
    return str(round(float(data), 50))


# this will format the input values
def find_data_type(data):
    modified = format_date(data)

    try:
        dt.datetime.strptime(modified, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        if is_number(data):
            modified = format_number(data)
    else:
        pass  # handle valid date

    return str(modified).strip()


# function to concat all the pk cols
def create_composite_key(row, pk_key):
    pk = ""
    for col in pk_key:
        pk = pk + str(row[col]) + "~"
    return pk[:-1]


# function that will format each col
def format_rows(row):
    updated_lst = []
    for col in row:
        updated_lst.append(find_data_type(str(col).strip()))

    return pd.Series(updated_lst)


def load_file(path, delimiter, header, skip_header):
    logger.info("Loading file :" + path)
    if len(header) > 0:
        df = pd.read_csv(path, delimiter=delimiter, names=header, engine='python', dtype=str)
    else:
        df = pd.read_csv(path, delimiter=delimiter, engine='python', dtype=str)

    if edmp_skip_header > 0:
        df = df[int(skip_header):]

    df.columns = map(str.lower, df.columns)

    logger.info("Sample DF record :\n" + str(df.iloc[0]) + "\n")
    return df


def create_report(html):
    htmllst = html.split("\n")
    isbody = False
    value1 = ""
    value2 = ""
    rowid = 0
    output = ""

    for line in htmllst:
        linefrmt = line.strip()
        if "<tbody>" in linefrmt:
            isbody = True

        if "</tbody>" in linefrmt:
            isbody = False

        if isbody and "<td>" in linefrmt:
            if rowid == 0:
                value1 = linefrmt
            elif rowid == 1:
                value2 = linefrmt

            if (value1.strip() != value2.strip()) and rowid == 1:
                line = linefrmt.replace("<td>", """<td bgcolor="#FF0000">""")

            if rowid == 0:
                rowid = 1
            else:
                rowid = 0
                value1 = ""
                value2 = ""

        output += line
    return output


def create_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = logging.FileHandler('log/DQMF.log')
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def process(tblname, pk_key_str, src_header_str, hdmp_header_str, edmp_path, src_path, src_delimiter,
            src_skip_header, edmp_delimiter, edmp_skip_header):
    report = ""
    summery = "<H2>Summary</H2>"

    pk_key = pk_key_str.lower().split(",")
    src_header = ""
    hdmp_header = ""

    if len(src_header_str) > 0:
        src_header = src_header_str.lower().split(",")

    if len(hdmp_header_str) > 0:
        hdmp_header = hdmp_header_str.lower().split(",")

    df_edmp_raw = load_file(edmp_path, edmp_delimiter, hdmp_header, edmp_skip_header)

    df_src_raw = load_file(src_path, src_delimiter, src_header, src_skip_header)

    logger.info("total number of rows in the SRC file : %s" % len(df_src_raw.index.values))
    logger.info("total number of rows in the EDMP file : %s" % len(df_edmp_raw.index.values))

    summery += "<p>" + "total number of rows in the SRC file : %s" % len(df_src_raw.index.values) + "</p>"
    summery += "<p>" + "total number of rows in the EDMP file : %s" % len(df_edmp_raw.index.values) + "</p>"

    key = "--" + "~".join(pk_key) + "--"

    df_src_raw[key] = df_src_raw.apply(lambda row: create_composite_key(row, pk_key), axis=1)
    logger.info("creating the PK for SRC:\n" + str(df_src_raw.iloc[0]) + "\n")

    df_edmp_raw[key] = df_edmp_raw.apply(lambda row: create_composite_key(row, pk_key), axis=1)
    logger.info("creating the PK for EDM:\n" + str(df_edmp_raw.iloc[0]) + "\n")

    srctotrows = len(df_src_raw.index.values)
    edmtotrows = len(df_edmp_raw.index.values)

    # checking if all the cols are matching
    edmp_cols = set(df_edmp_raw.columns)
    src_cols = set(df_src_raw.columns)

    matchingCol = list(edmp_cols.intersection(src_cols))
    edmpXCols = [item for item in list(edmp_cols) if item not in matchingCol]
    srcXCols = [item for item in list(src_cols) if item not in matchingCol]

    if len(edmpXCols) == 0:
        logger.info("all the EDMp cols are present in SRC")
    else:
        logger.info("EDMP cols that are not present in SRC :" + ",".join(edmpXCols))

    if len(srcXCols) == 0:
        logger.info("all the SRC cols are present in EDMP")
    else:
        logger.info("SRC cols that are not present in the EDMP :" + ",".join(srcXCols))

    # updating the data frames so that both contain the matching cols only
    df_src_raw = df_src_raw[matchingCol]
    df_edmp_raw = df_edmp_raw[matchingCol]

    logger.info("SRC DF with only matching cols :\n" + str(df_src_raw.iloc[0]) + "\n")
    logger.info("EDM DF with only matching cols :\n" + str(df_edmp_raw.iloc[0]) + "\n")

    # formatting all the cell
    edmp_header = pd.Series(df_edmp_raw.columns)
    df_edmp = df_edmp_raw.apply(lambda row: format_rows(row), axis=1)
    df_edmp.rename(columns=edmp_header, inplace=True)

    logger.info("EDM DF with foramted cell : \n" + str(df_edmp.iloc[0]) + "\n")

    src_header = pd.Series(df_src_raw.columns)
    df_src = df_src_raw.apply(lambda row: format_rows(row), axis=1)
    df_src.rename(columns=src_header, inplace=True)

    logger.info("SRC DF with foramted cell : \n" + str(df_src.iloc[0]) + "\n")

    # making the PK as the index for both the tables
    df_src.set_index(key, inplace=True)
    df_edmp.set_index(key, inplace=True)
    df_src.sort_index(inplace=True)
    df_edmp.sort_index(inplace=True)

    # checking and removing the duplicate rows
    src_duplicate = df_src[df_src.index.duplicated(keep=False)]
    EDMP_duplicate = df_edmp[df_edmp.index.duplicated(keep=False)]

    srcDup = 0
    edmDup = 0

    if len(src_duplicate.index.values) > 0:
        logger.info("%s duplicates found in the SRC file" % (len(src_duplicate.index.values) / 2))
        report += "<H2>Duplicates found in the SRC file</H2>" + src_duplicate.to_html()
        summery += "<p>" + "Duplicates found in the SRC file: %s" % (len(src_duplicate.index.values) / 2) + "</p>"
        srcDup = len(src_duplicate.index.values) / 2

    if len(EDMP_duplicate.index.values) > 0:
        logger.info("%s duplicates found in the EDMP file" % (len(EDMP_duplicate.index.values) / 2))
        report += "<H2>Duplicates found in the EDMP file</H2>" + EDMP_duplicate.to_html()
        summery += "<p>" + "Duplicates found in the EDMP file: %s" % (len(EDMP_duplicate.index.values) / 2) + "</p>"
        edmDup = len(EDMP_duplicate.index.values) / 2

    df_src = df_src.drop_duplicates(pk_key)
    df_edmp = df_edmp.drop_duplicates(pk_key)

    # reporting out the PKs that are not present in both the files
    SRCmissing = df_src[~df_src.isin(df_edmp)].dropna()
    EDMPmissing = df_edmp[~df_edmp.isin(df_src)].dropna()

    srcMissing = 0
    edmMissing = 0
    if len(SRCmissing.index.values) == 0:
        logger.info("SRC file has all the PK in EDMP file")
    else:
        logger.info("SRC file has some records that do not have matching PK in EDMP, please check output ")
        report += "<H2>SRC file has some records that do not have matching PK in EDMP</H2>" \
                  + SRCmissing.to_html()
        summery += "<p>" + "Extra SRC records: %s " % len(SRCmissing.index.values) + " </p> "
        srcMissing = len(SRCmissing.index.values)

    if len(EDMPmissing.index.values) == 0:
        logger.info("EDMP file has all the PK in SRC file")
    else:
        logger.info("EDMP file has some records that do not have matching PK in SRC, please check output ")
        report += "<H2>EDMP file has some records that do not have matching PK in SRC</H2>" \
                  + EDMPmissing.to_html()
        summery += "<p>" + "Extra EDMP records: %s " % len(EDMPmissing.index.values) + " </p> "
        edmMissing = len(EDMPmissing.index.values)

    # updating the DFs so that both of them have common PKs
    df_src = df_src[df_src.index.isin(df_edmp.index)]
    df_edmp = df_edmp[df_edmp.index.isin(df_src.index)]

    logger.info("the number of records being matched : (SRC:%s, EDMP: %s)" % (str(len(df_src.index.values)),
                                                                        str(len(df_edmp.index.values))))
    mismatch = 0
    if len(df_edmp.index.values) != 0:
        # comparing all the cols and finding out the differences
        ne = (df_edmp != df_src).any(1)
        df_all = pd.concat([df_src[ne], df_edmp[ne]], axis='columns', keys=['SRC', 'EDMP'])

        df_final = df_all.swaplevel(axis='columns')[df_edmp.columns[:]]

        # starting process to create the output report
        if len(df_final.index.values) == 0:
            logger.info("the files are identical")
            report += "<H1>Report</H1><p>the files are identical</p>"
        else:
            out = create_report(df_final.to_html())
            report += "<H2>Report</H2>" + out
            logger.info("%s records have mismatch, please check the report" % str(len(df_final.index.values)))
            mismatch = len(df_final.index.values)
    else:
        report += "<H1>Report</H1><p>None rows to match</p>"
        logger.info("None rows to match")

    file = open("report/" + tblname + '_Final_report.html', 'w')
    file.write("<H1>" + tblname + " Final Report</H1>")
    file.write(summery)
    file.write(report)
    file.close()
    return srctotrows, edmtotrows, srcDup, edmDup, srcMissing, edmMissing, mismatch


if __name__ == "__main__":
    parser = SafeConfigParser()
    logger = create_logger()

    if os.path.exists("config/config.ini") and os.path.exists("config/table_details.xlsx"):
        parser.read('config/config.ini')
        config_df = pd.read_excel("./config/table_details.xlsx")
        config_df.fillna("", inplace=True)
    else:
        print("could not find the config files")
        sys.exit()

    if os.path.exists("running.script"):
        print("Stoping execution as another process running!!")
        sys.exit()
    else:
        open('running.script', "a").close()

    src_base_path, src_delimiter, src_skip_header = read_variable("src")
    edmp_base_path, edmp_delimiter, edmp_skip_header = read_variable("edm")
    total_status = []
    for item in config_df.index:
        row = config_df.iloc[item]
        tablename = row[0]
        pk = row[1]
        src_file_name = src_base_path + "/" + row[2]
        SRC_Cols = row[3]
        edmp_file_name = edmp_base_path + "/" + row[4]
        EDMP_cols = row[5]
        Ignore_Cols = row[6]
        logger.info('*************************************************************************************************')
        logger.info('Starting the process with the following config :')
        logger.info('tablename : ' + tablename)
        logger.info('Primary Key : ' + pk)
        logger.info('SRC col list :' + SRC_Cols)
        logger.info('EDM col list :' + EDMP_cols)
        logger.info('SRC file name :' + src_file_name)
        logger.debug('EDM file name :' + edmp_file_name)
        srctotrows, edmtotrows, srcDup, edmDup, srcMissing, edmMissing, mismatch = 0, 0, 0, 0, 0, 0, 0

        try:
            srctotrows, edmtotrows, srcDup, edmDup, srcMissing, edmMissing, mismatch = \
                process(tablename, pk, SRC_Cols, EDMP_cols, edmp_file_name,
                        src_file_name, src_delimiter, src_skip_header, edmp_delimiter, edmp_skip_header)

            status = "Success"
        except Exception as e:
            logger.error("error for " + tablename + ": " + str(e))
            status = "Fail"

        total_status.append(
            (tablename, srctotrows, edmtotrows, srcDup, edmDup, srcMissing, edmMissing, mismatch, status))

    jobStatus =pd.DataFrame.from_records(total_status, columns=["Table name", "Total source rows", "Total edm rows",
                                                     "SRC duplicates", "EDM duplicates", "SRC missing", "EDM missing",
                                                     "Mismatch", "Status"])
    jobStatus.to_html("report/JobStatus.html")

    os.remove('running.script')
