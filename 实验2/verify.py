#!/usr/bin/env python3
"""Independent, single-process reference; checks every posting and total."""
import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import re


def reference(directory, stopwords):
    stops = {s.strip().lower() for s in Path(stopwords).read_text().splitlines()
             if s.strip() and not s.strip().startswith('#')}
    index = defaultdict(dict)
    raw = removed = retained = 0
    documents = []
    for path in sorted(Path(directory).glob('*.txt')):
        # Match Hadoop FileInputFormat, which skips hidden/metadata files.
        if path.name.startswith(('.', '_')):
            continue
        tokens = re.findall(r'[a-z]+', path.read_text(encoding='utf-8').lower())
        counts = Counter(w for w in tokens if w not in stops)
        raw += len(tokens)
        removed += sum(w in stops for w in tokens)
        retained += sum(counts.values())
        documents.append({'document': path.name, 'raw_tokens': len(tokens),
                          'indexed_tokens': sum(counts.values()), 'unique_terms': len(counts)})
        for word, count in counts.items():
            index[word][path.name] = count
    return dict(index), {'documents': documents, 'document_count': len(documents),
                        'stopword_count': len(stops), 'raw_tokens': raw,
                        'removed_tokens': removed, 'indexed_tokens': retained,
                        'unique_terms': len(index),
                        'posting_count': sum(len(v) for v in index.values())}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('documents')
    parser.add_argument('stopwords')
    parser.add_argument('index')
    parser.add_argument('--json')
    args = parser.parse_args()
    expected, stats = reference(args.documents, args.stopwords)
    actual = {}
    previous = None
    for line in Path(args.index).read_text(encoding='utf-8').splitlines():
        word, postings = line.split('\t', 1)
        if previous is not None and word <= previous:
            raise AssertionError(f'Duplicate or unsorted word: {word}')
        previous = word
        matches = re.findall(r'<([^,<>]+),(\d+)>', postings)
        if not matches or matches[-1][0] != 'total':
            raise AssertionError(f'Missing total: {word}')
        canonical = ';'.join(f'<{d},{c}>' for d, c in matches) + '.'
        if canonical != postings:
            raise AssertionError(f'Invalid output format: {word}')
        pairs = matches[:-1]
        docs = {d: int(c) for d, c in pairs}
        if len(docs) != len(pairs) or list(docs) != sorted(docs):
            raise AssertionError(f'Duplicate or unsorted posting: {word}')
        if any(c <= 0 for c in docs.values()) or sum(docs.values()) != int(matches[-1][1]):
            raise AssertionError(f'Invalid frequency or total: {word}')
        actual[word] = docs
    if actual != expected:
        mismatches = [w for w in sorted(set(actual) | set(expected))
                      if actual.get(w) != expected.get(w)]
        raise AssertionError(f'{len(mismatches)} mismatches: {mismatches[:10]}')
    stats['verified'] = True
    stats['top_terms'] = sorted(
        [{'word': w, 'total': sum(v.values()), 'document_frequency': len(v)}
         for w, v in actual.items()], key=lambda t: (-t['total'], t['word']))[:20]
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    if args.json:
        Path(args.json).write_text(json.dumps(stats, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()
