#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import io
import os
import traceback
import argparse
import psycopg2
from datetime import datetime
import bz2
import re
import glob
import ujson as json
from psycopg2.extras import Json
from itertools import islice
from dateutil.relativedelta import relativedelta
from dateutil import parser as du_parser
import time
import calendar
from ezconf import ConfigFile
from utils.db import DB
from utils.dateutils import convertRFC822ToDateTime


from utils.status_updater import *



DB_FIELDS = [
    "id",
    "text",
    "created_at",
    "geo_coordinates_0",
    "geo_coordinates_1",
    "place_full_name",
    "hashtags",
    "urls",
    "truncated",
    "source",
    "user_id",
    "user_screen_name",
    "name",
    "description",
    "location",
    "utc_offset",
    "time_zone",
    "in_reply_to_screen_name",
    "in_reply_to_user_id",
    "retweet_count",
    "quote_count",
    "reply_count",
    "favorite_count",
    "user_geo_coordinates_0",
    "user_geo_coordinates_1",
    "user_lang",
    "user_statuses_count",
    "user_followers_count",
    "user_friends_count",
    "user_created_at",
    "user_protected",
    "user_geo_enabled",
    "retweeted_status_id",
    "retweeted_status_text",
    "retweeted_status_user_id",
    "retweeted_status_user_screen_name",
    "quoted_status_id",
    "quoted_status_text",
    "quoted_status_user_id",
    "quoted_status_user_screen_name",
    "source_tweet_id",
    "collection_date",
    "collection_geo",
    "collection_locterm",
    "tweet",
]



def get_expanded_urls(obj):
    urls = obj["entities"]["urls"]

    if len(urls) == 0:
        return None

    return [u["expanded_url"] for u in urls]



def get_hashtags(obj):
    hashtags = obj["entities"]["hashtags"]

    if len(hashtags) == 0:
        return None

    return [h["text"] for h in hashtags]


def sanitize_string(text):
    return text.replace('\\u0000', '')

def create_insert_tuple(tweet_obj):
    #print line
    #print "\n" * 4

    obj = tweet_obj.obj

    try:

        user = obj.get("user")
        if user is None:
            print "\n" * 100
            print json.dumps(obj, indent=4)
            print "\n" * 4
            print obj.keys()
            return None

        row = (
            obj["id"],
            obj["text"],
            convertRFC822ToDateTime(obj["created_at"]),
            obj["geo"]["coordinates"][0] if obj["geo"] is not None else None,
            obj["geo"]["coordinates"][1] if obj["geo"] is not None else None,
            obj["place"]["full_name"] if obj["place"] is not None else None,
            get_hashtags(obj),
            get_expanded_urls(obj),
            obj["truncated"],
            obj["source"],
            user["id"],
            user["screen_name"],
            user["name"],
            user["description"],
            user["location"],
            user["utc_offset"],
            user["time_zone"],
            obj["in_reply_to_screen_name"],
            obj["in_reply_to_user_id"],
            obj["retweet_count"],
            obj.get("quote_count"),
            obj.get("reply_count"),
            obj["favorite_count"],
            user["coordinates"][0] if user.get("geo") is not None else None,
            user["coordinates"][1] if user.get("geo") is not None else None,
            user["lang"],
            user["statuses_count"],
            user["followers_count"],
            user["friends_count"],
            user["created_at"],
            user["protected"],
            user["geo_enabled"])

        retweet = obj.get("retweeted_status", None)
        if retweet is not None:
            row += (
                retweet["id"],
                retweet["text"],
                retweet["user"]["id"],
                retweet["user"]["screen_name"],
                )
        else:
            row += (
                None,
                None,
                None,
                None)


        quote = obj.get("quoted_status", None)
        if quote is not None:
            row += (
                quote["id"],
                quote["text"],
                quote["user"]["id"],
                quote["user"]["screen_name"],
                )
        else:
            row += (
                None,
                None,
                None,
                None)


        # add dataset fields
        row += (
            None,
            tweet_obj.collection_date,
            tweet_obj.geo,  
            tweet_obj.locterm,
            sanitize_string(json.dumps(obj))
            )

        return row

    except Exception, e:
        print "\n"* 4
        print e
        print json.dumps(obj, indent=4)



        raise e



class CrispTweet(object):
    """
    Container for a tweet from the crisp project
    """

    def __init__(self, collection_date, is_locterm, is_geo,  id, obj):
        """
        """

        self.id = id
        self.obj = obj
        self.collection_date = collection_date
        self.locterm = is_locterm
        self.geo = is_geo

    def update(self, is_locterm, is_geo):
        """
        convenience function update the tweet with new collection data.

        This is incredibly problematic when mixing these two datasets.
        Which collection_date tweet takes precedence?
        """

        self.locterm |= is_locterm
        self.geo |= is_geo




class CrispTimeline(object):
    """
    Convenience wrapper for Tweets
    """

    def __init__(self):
        self.timeline = {}

    def add(self, collection_date, is_locterm, is_geo, id, obj):
        if id in self.timeline:
            self.timeline[id].update(is_locterm, is_geo)
        else:
            self.timeline[id] = CrispTweet(
                collection_date,
                is_locterm,
                is_geo,
                id,
                obj)

    def parse_file(self, filename, is_locterm, is_geo):
        """
        opens filename and parses it
        """

        with open(filename) as f:
            data = json.loads(f.read())
            collected_ts = du_parser.parse(data["utc_timestamp"], ignoretz=True)
            for t in data["historic_tweets"]:
                id = t["id"]
                self.add(collected_ts, is_locterm, is_geo, id, t)




class TimelineFileIterator(object):
    """

    """

    def __init__(self, filename):
        """
        """
        if filename.endswith(".bz2"):
            print "detected bz2 file..."
            self.file = bz2.BZ2File(filename, "r", 1024*1024*32)
            self.file_length = 0
        else:
            self.file = open(filename, "r")

            # get file length
            self.file.seek(0, os.SEEK_END)
            self.file_length = self.file.tell()
            self.file.seek(0, os.SEEK_SET)

        if self.file is not None:
            obj = json.loads(self.file.read())
            self.objects = obj["historic_tweets"]
            self.iter = iter(self.objects)


    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        cleanup
        """
        # close
        self.file.close()

    def __iter__(self):
        return self


    def next(self):
        """
        iterates to next item
        """
        return next(self.iter)

    def fileno(self):
        """
        returns fileno
        """
        return self.file.fileno()




def create_table(db, table_name):
    with open("./sql/create_crisp.sql") as f:
        sql = f.read().format(table_name=table_name)
        db.execute(sql)




added_ids = set()


#
#
# program arguments
#
#
parser = argparse.ArgumentParser(description='twitter postgres insert code')
parser.add_argument('locterm_data_dir', help='directory where the locterm timelines exist')
parser.add_argument('geo_data_dir', help='directory where geo timelines exist')
parser.add_argument('table_name', help='name of the table')
parser.add_argument(
    '--drop_table', help='drops table if it exists', action='store_true')
parser.add_argument(
    '--create_table', help='creates table if it exist', action='store_true')
parser.add_argument(
    '--batch_size', help='number of items per insert call', default=10)
parser.add_argument(
    "--configfile", default="config.json", help="config file to use")

# add db args with no default database
DB.add_args(parser, None)


args = parser.parse_args()

# config file
config = ConfigFile(args.configfile)

# get db connection
conn = DB(args=args, configfile=config)
cur = conn.get_cursor()


# first drop table if needed
if args.drop_table:
    print "Dropping Table..."
    cur.execute("DROP TABLE IF EXISTS {};".format(args.table_name))


# Create table
if args.create_table:
    print "Creating table..."
    create_table(cur, args.table_name)


# status updater
status_updater = StatusUpdater()


# find all our files
# this works by joining the filenames, which are just id.json, from 2 directories
# this may run into case-sensitivity issues on some platforms
input_files = sorted(
    set(
        [os.path.basename(x) for x in glob.glob(os.path.join(args.locterm_data_dir, "*.json"))] +
        [os.path.basename(x) for x in glob.glob(os.path.join(args.geo_data_dir, "*.json"))]
        )
    )

status_updater.total_files = len(input_files)


# set timezone
cur.execute("SET TIME ZONE 'UTC';")


arg_list = "(%s)"%(",".join(["%s"] * len(DB_FIELDS)))
db_cols = "(%s)"%(",".join(DB_FIELDS))

possible_data_dirs = [args.locterm_data_dir, args.geo_data_dir]

# step through each file
for in_idx, infile_name in enumerate(input_files):

    # update status_updater
    status_updater.current_file = in_idx
    status_updater.update(force=True)

    ct = CrispTimeline()

    # add each timeline from all datasets if it exists
    for data_dir in possible_data_dirs:
        file_path = os.path.join(data_dir, infile_name)
        is_geo = args.geo_data_dir == data_dir
        is_locterm = args.locterm_data_dir == data_dir
        if os.path.exists(file_path):
            ct.parse_file(file_path, is_locterm, is_geo)


    # sort the data
    ids = sorted(ct.timeline.keys())


    status_updater.total_val = len(ids)
    status_updater.current_val = 0

    try:
        id_iter = iter(ids)

        while True:
            lines = [
                create_insert_tuple(
                    ct.timeline[i]) for i in islice(id_iter, 10)]

            # enough is enough
            if lines is None or len(lines) == 0:
                break

            # update parsed status updater
            status_updater.count += len(lines)

            try:
                values = ','.join(
                    cur.mogrify(arg_list, x) for x in lines)
                query = "insert into %s %s values " % (
                    args.table_name, db_cols)
                cur.execute(query + values)

                # update status updater
                status_updater.total_added += len(lines)
                status_updater.current_val += len(lines)

            except Exception, e:
                #print "\n"*2, values, "\n"*2, query
                traceback.print_exc()
                quit()


            
            status_updater.update()

    except Exception, e:
        traceback.print_exc()
        quit()


    status_updater.update()


status_updater.update(force=True)

# commit the transaction
print "Committing..."
conn.commit()
