#!/usr/bin/env python3
"""Download 12 public English Apache Hadoop 3.3.6 documentation pages."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
from html.parser import HTMLParser
import json
from pathlib import Path
import re
from urllib.request import Request, urlopen

BASE = 'https://hadoop.apache.org/docs/r3.3.6/'
PAGES = [
    ('Single Cluster Setup', 'hadoop-project-dist/hadoop-common/SingleCluster.html'),
    ('Cluster Setup', 'hadoop-project-dist/hadoop-common/ClusterSetup.html'),
    ('File System Shell', 'hadoop-project-dist/hadoop-common/FileSystemShell.html'),
    ('HDFS Architecture', 'hadoop-project-dist/hadoop-hdfs/HdfsDesign.html'),
    ('HDFS User Guide', 'hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html'),
    ('HDFS Permissions Guide', 'hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html'),
    ('MapReduce Tutorial', 'hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html'),
    ('Apache Hadoop YARN', 'hadoop-yarn/hadoop-yarn-site/YARN.html'),
    ('Capacity Scheduler', 'hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html'),
    ('Fair Scheduler', 'hadoop-yarn/hadoop-yarn-site/FairScheduler.html'),
    ('Hadoop Archives', 'hadoop-archives/HadoopArchives.html'),
    ('WebHDFS REST API', 'hadoop-project-dist/hadoop-hdfs/WebHDFS.html'),
]


class ArticleParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.parts = []
        self.capturing = False
        self.capture_depth = None
        self.skip = 0

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        # Maven's contentBox is the article; menus and page chrome are outside.
        if attrs.get('id') == 'contentBox':
            self.capturing = True
            self.capture_depth = len(self.stack)
        if tag in {'br', 'hr', 'img', 'meta', 'link', 'input'}:
            if self.capturing and tag == 'br': self.parts.append('\n')
            return
        self.stack.append(tag)
        if tag in {'script', 'style', 'nav', 'footer'}: self.skip += 1
        if self.capturing and tag in {'p', 'div', 'li', 'tr', 'h1', 'h2', 'h3', 'h4', 'pre', 'dt', 'dd'}:
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'nav', 'footer'}: self.skip = max(0, self.skip - 1)
        if self.capturing and tag in {'p', 'div', 'li', 'tr', 'h1', 'h2', 'h3', 'h4', 'pre', 'dt', 'dd'}:
            self.parts.append('\n')
        if tag in self.stack:
            at = len(self.stack) - 1 - self.stack[::-1].index(tag)
            del self.stack[at:]
        if self.capturing and len(self.stack) == self.capture_depth:
            self.capturing = False

    def handle_data(self, data):
        if self.capturing and not self.skip: self.parts.append(data)

    def article(self):
        lines = [re.sub(r'\s+', ' ', x).strip() for x in ''.join(self.parts).splitlines()]
        text = '\n'.join(x for x in lines if x) + '\n'
        if len(re.findall(r'[a-z]+', text.lower())) < 100:
            raise ValueError('Article extraction returned fewer than 100 English tokens')
        return text


def download(item):
    number, (title, relative) = item
    url = BASE + relative
    request = Request(url, headers={'User-Agent': 'Hadoop-Inverted-Index-Course-Lab/1.0'})
    with urlopen(request, timeout=60) as response:
        html = response.read().decode('utf-8')
    parser = ArticleParser()
    parser.feed(html)
    text = parser.article()
    filename = f'document{number:02d}.txt'
    payload = text.encode('utf-8')
    (Path(__file__).parent / 'documents' / filename).write_bytes(payload)
    return {'document': filename, 'title': title, 'url': url,
            'downloaded_at_utc': datetime.now(timezone.utc).isoformat(timespec='seconds'),
            'sha256': hashlib.sha256(payload).hexdigest(), 'bytes': len(payload),
            'raw_tokens': len(re.findall(r'[a-z]+', text.lower()))}


def main():
    root = Path(__file__).parent
    (root / 'documents').mkdir(exist_ok=True)
    with ThreadPoolExecutor(max_workers=3) as pool:
        manifest = list(pool.map(download, enumerate(PAGES, 1)))
    (root / 'sources.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + '\n')
    for item in manifest:
        print(f"{item['document']}: {item['raw_tokens']} tokens | {item['title']}")
    print(f'Downloaded {len(manifest)} public English documents.')


if __name__ == '__main__':
    main()
