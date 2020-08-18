#!/usr/bin/env python3

import time
import cmd
import sys
import gzip
import functools
import re
import xml.etree.ElementTree as ElementTree

from typing import List, Set, Dict, Iterator

from tqdm import tqdm

class Document:
    def __init__(self, doc_id, title, url, abstract):
        self.doc_id = doc_id
        self.title = title
        self.url = url
        self.abstract = abstract

    def __repr__(self):
        return '<Document id = "{}", title = "{}", url = "{}", abstract = "{}">'.format(
            self.doc_id, self.title, self.url, self.abstract)


def measure_time(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print("Elapsed time: {} seconds".format(end_time - start_time))
        return result
    return wrapper


def load_documents(file_path: str) -> Iterator[Document]:
    doc_id = 0
    with gzip.open(file_path, "r") as input:
        tree = ElementTree.iterparse(input)
        for event, elem in tree:
            if elem.tag == "doc":
                doc_id += 1
                title = elem.find('title').text
                url = elem.find('url').text
                abstract = elem.find('abstract').text
                yield Document(doc_id, title, url, abstract)


def tokenizer(text: str) -> List[str]:
    return re.findall(r"\w[\w']*\w|\w", text)


def filter_stopwords(tokens: List[str]) -> List[str]:
    global stopwords
    if not stopwords:
        stopwords = set()
        with open('stopwords.txt') as f:
            stopwords = set([w.strip('\n') for w in f.readlines()])
    return list(filter(lambda w: w not in stopwords, tokens))


def analyze(text: str) -> List[str]:
    if text is None or len(text) == 0:
        return []

    from nltk.stem import PorterStemmer
    stemmer = PorterStemmer()
    tokens = filter_stopwords([token.lower() for token in tokenizer(text)])
    tokens = [stemmer.stem(w) for w in tokens]
    return tokens


@measure_time
def index_documents(docs: List[Document]):
    global index
    for doc in tqdm(docs):
        for token in analyze(doc.abstract):
            if (token in index) and index[token][-1] == doc.doc_id:
                continue
            index.setdefault(token, []).append(doc.doc_id)


@measure_time
def search(term: str) -> List[Document]:
    doc_idx = []
    for token in analyze(term):
        if token in index:
            doc_idx.append(set(index[token]))
    return doc_idx


class FTSShell(cmd.Cmd):
    intro = 'Full text search. Type help or ? to list commands.\n'
    prompt = '>> '
    data = {'wikipedia': 'enwiki-latest-abstract1.xml.gz'}

    def do_data(self, arg):
        'Show all text data'
        print(data)

    def do_load(self, arg):
        'Load data for search'
        if arg not in FTSShell.data:
            print("Data does not exist! Please choose below dataset")
            print(FTSShell.data)
            return
        self.data = FTSShell.data[arg]
        print("Loading data [{}] ...".format(self.data))
        self.docs_iterator = load_documents(self.data)

    def do_index(self, arg):
        'Index loaded data'
        self.docs = {}
        for doc in self.docs_iterator:
            self.docs[doc.doc_id] = doc
        index_documents(self.docs.values())

    def do_search(self, arg):
        'Search for keywords'
        try:
            print("Searching for: {} in {}".format(arg, self.data))
            result_sets = search(arg)
            result = set.intersection(*result_sets)
            print("====== Found {} documents ======".format(len(result)))
            for ids in result:
                print(self.docs[ids])

        except AttributeError:
            print("Data needed to be loaded before searching. [help load] for more detail")

    def do_EOF(self, arg):
        'Return from this shell'
        print('\nGood bye!')
        return True

    def emptyline(self):
        pass


if __name__ == "__main__":
    index = dict()
    stopwords = set()
    FTSShell().cmdloop()