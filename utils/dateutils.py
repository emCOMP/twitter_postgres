#!/usr/bin/env python
# -*- coding: utf-8 -*-


import email.utils
from datetime import datetime

#
# convert an RFC822 date to a datetime
# borrowed and modified from: https://gist.github.com/robertklep/2928188
#
# original used fromtimestamp instead of utcfromtimestamp, which converts it
# to local time. currently, all of the tweets seem to just use utc, so we
# can ignore the timestamp (though it's unclear if mktime_tz actually
# utilizes the info or just ignores it. )
#
def convertRFC822ToDateTime(rfc822string):
    """
        convert an RFC822 date to a datetime
    """
    return datetime.utcfromtimestamp(
        email.utils.mktime_tz(
            email.utils.parsedate_tz(
                rfc822string
                )
            )
        )
