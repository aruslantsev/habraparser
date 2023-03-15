import datetime

import requests
import time
from habraparser.constraints import POST_URL
import json
import pypika
from pypika import Query, Column
import mysql.connector


class Parser:
    def __init__(self, mysql_creds, codes_table):
        self.creds = mysql_creds
        self.codes_table = codes_table
        # run all tddl here

    def get_post(self, post_id: int):
        date_now = datetime.datetime.now()
        timestamp = int(time.time())
        request = requests.get(POST_URL.format(post_id), timeout=30)
        status_code = request.status_code

        connection = mysql.connector.connect(**self.creds, database=None)
        cursor = connection.cursor()
        query = Query.into(self.codes_table).insert(post_id, status_code).get_sql()
        cursor.execute(query)

        # put to status table
        print(post_id, status_code)

        if status_code == 200:
            post_data = json.loads(request.text)
            is_corporative = post_data["isCorporative"]
            time_published = post_data["timePublished"]
            lang = post_data["lang"]
            title = post_data["titleHtml"]
            author = post_data["author"]
            author_id = author["id"]
            author_alias = author["alias"]
            author_name = author["fullname"]
            text = post_data["textHtml"]
            status = post_data["status"]

            hubs = post_data["hubs"]  # [id, alias, type, title, titleHtml]
            tags = post_data["tags"]  # [{'titleHtml': ...}]

            metadata_query = Query.into(self.meta_table).insert(
                post_id, time_published, lang, author_id, is_corporative, status
            )

            authors_query = Query.into(self.authors_table).insert(
                author_id, author_alias, author_name
            )

            texts_query = Query.into(self.texts_table).insert(
                post_id, title, text
            )

            for tag in tags:
                tags_query = Query.into(self.tags_table).insert(
                    post_id, tag["titleHtml"]
                )

            for hub in hubs:
                hubs_query = Query.into(self.hubs_table).insert(
                    post_id, hub["id"]
                )
                hubs_meta_query = Query.into(self.hubs_meta_table).insert(
                    hub["id"], hub["alias"], hub["type"], hub["title"], hub["titleHtml"]
                )

        connection.commit()
