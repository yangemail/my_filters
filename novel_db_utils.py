# -*- coding: utf-8 -*-
import json
import pprint
import pymongo

from pymongo import MongoClient
import pymongo.errors as mongoerr


class BaseDbObject(object):
    def __init__(self):
        self.pp = pprint.PrettyPrinter(indent=4)
        try:
            self.client = MongoClient(host='localhost:27017', connect=True)
            self.database = self.client.get_database('entertainment')
        except mongoerr.ConnectionFailure as e:
            print("Could not connect: %s" % e)

    def __del__(self):
        if self.client is not None:
            self.client.close()

    def insert_target_book(self, book_coll, book):
        return book_coll.insert_one(book, bypass_document_validation=True).inserted_id

    def insert_many_target_chapters(self, chapter_coll, chapters):
        return chapter_coll.insert_many(chapters, bypass_document_validation=True)


class B23usCom(BaseDbObject):
    def __init__(self):
        super(B23usCom, self).__init__()

    def run(self):
        self.on_operation_books()

    def on_operation_books(self):
        source_book_coll = self.database['books']
        for source_book in source_book_coll.find({}):
            self.pp.pprint(source_book)

            # book operations
            target_book = dict({
                "book_summary": source_book["book_summary"],
                "book_name": source_book["book_name"],
                "book_author": source_book["book_author"],
                "book_serial_wordcount": source_book["book_serial_wordcount"],
                "book_category": source_book["book_category"],
                "book_last_update_time": source_book["book_last_update_time"],
                "book_id": source_book["book_id"],
                "book_chapters_url": source_book["book_chapters_url"],
                "book_serial_status": source_book["book_serial_status"]
            })
            target_book['_id'] = self.insert_target_book(book_coll=self.database['target_books'], book=target_book)

            # chapter operations
            self.on_operation_chapters(book=target_book)

    def on_operation_chapters(self, book):
        print('print target book {}', book)
        self.pp.pprint(book)
        source_chapter_coll = self.database['chapters']
        chapters = []

        for source_chapter in source_chapter_coll.find({'book_id': book['book_id']}):
            new_chapter = dict({
                "chapter_sequence": source_chapter["chapter_sequence"],
                "chapter_is_update_required": source_chapter["chapter_is_update_required"],
                "chapter_id": source_chapter["chapter_id"],
                "chapter_last_update_time": source_chapter["chapter_last_update_time"],
                "book_id": book["_id"],
                "chapter_seq_chinese": source_chapter["chapter_seq_chinese"],
                "chapter_content": source_chapter["chapter_content"],
                "chapter_url": source_chapter["chapter_url"],
            })
            chapters.append(new_chapter)
        result = self.insert_many_target_chapters(chapter_coll=self.database['target_chapters'], chapters=chapters)
        self.pp.pprint(len(result.inserted_ids))

    def __del__(self):
        super(B23usCom, self).__del__()


if __name__ == '__main__':
    b23us_com = B23usCom()
    for i in range(10):
        b23us_com.run()

