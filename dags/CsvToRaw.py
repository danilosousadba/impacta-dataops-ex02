from br.com.danilosousa.common import ReadCsv,WriteParquet
import logging
import os
import sys

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


def main(file,sep,header,raw,compression):
    logger.info("file : {}".format(file))
    logger.info("sep : {}".format(sep))
    logger.info("header : {}".format(header))
    logger.info("raw : {}".format(raw))
    logger.info("compression : {}".format(compression))

    df = ReadCsv(file,sep,header)
    df.info()

    print(df.head(2))

    WriteParquet(df,raw,compression)


main()