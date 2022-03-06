from pathlib import Path
import multiprocessing
from homer.analyzer import Article
from homer.cmdline_printer import ArticlePrinter
import textstat
import pandas as pd
import nltk
import csv
import os
from tqdm import tqdm

nltk.download('punkt')
nltk.download('cmudict')
nltk.download('stopwords')

columns = [
    'AccessionID',
    'reading_time', 
    'flesch_reading_score', 
    'dale_chall_readability_score',
    'total_paragraphs',
    'avg_sentences_per_para',
    'len_of_longest_paragraph',
    'total_sentences',
    'avg_words_per_sentence',
    'len_of_longest_sentence',
    'total_words',
    'compulsive_hedgers',
    'intensifiers',
    'and_frequency',
    'vague_words_count',
    'vague_words_frequency',
    'polysyllab_count',
    'polysyllab_frequency'
]

def generate_features(filepath):
    with open(filepath, "r", errors="ignore") as f:
        text = f.read()
    
    article = Article('Article name', 'Author', text)

    reading_time = article.reading_time
    flesch_reading_score = textstat.flesch_reading_ease(text)
    dale_chall_readability_score = round(textstat.dale_chall_readability_score(text), 1)
    total_paragraphs = article.total_paragraphs
    avg_sentences_per_para = article.avg_sentences_per_para
    # standard deviation
    len_of_longest_paragraph = article.len_of_longest_paragraph
    total_sentences = article.total_sentences
    avg_words_per_sentence = article.avg_words_per_sentence
    len_of_longest_sentence = article.len_of_longest_sentence
    total_words = article.total_words
    compulsive_hedgers = len(article.get_compulsive_hedgers())
    intensifiers = len(article.get_intensifiers())
    and_frequency = float(article.get_and_frequency().replace(" %",""))
    vague_words_count = len(article.get_vague_words())
    vague_words_frequency = vague_words_count/total_words
    polysyllab_count = textstat.polysyllabcount(text)
    polysyllab_frequency = polysyllab_count/total_words

    return (
        reading_time, 
        flesch_reading_score, 
        dale_chall_readability_score, 
        total_paragraphs, 
        avg_sentences_per_para, 
        len_of_longest_paragraph,
        total_sentences,
        avg_words_per_sentence,
        len_of_longest_sentence,
        total_words,
        compulsive_hedgers,
        intensifiers,
        and_frequency,
        vague_words_count,
        vague_words_frequency,
        polysyllab_count,
        polysyllab_frequency
    )


def job(filepath):
    accession_id = os.path.splitext(os.path.basename(filepath))[0]
    try:
        features = generate_features(filepath)
    except Exception as e:
        return accession_id, None, e
    else:
        result = [accession_id]
        result.extend(features)
        return accession_id, result, None


if __name__ == "__main__":
    files = [os.path.join(dirname, filename) for dirname, _, filenames in os.walk("data") for filename in filenames if filename.endswith(".txt")]
    process_count = multiprocessing.cpu_count()
    if not os.path.exists("feature_progress.csv"):
        Path("feature_progress.csv").touch()

    with open("feature_progress.csv", "r") as f:
        progress = {}
        progress_reader = csv.reader(f)
        for row in progress_reader:
            progress[row[0]] = row[1]

    with open("features.csv", "a+") as f,\
         open("feature_progress.csv", "a+") as p_f,\
         multiprocessing.Pool(multiprocessing.cpu_count()) as pool:
        
        writer = csv.writer(f)
        if f.tell() == 0:
            writer.writerow(columns)
        
        progress_writer = csv.writer(p_f)
        
        files = [
            file 
            for file in files 
            if os.path.splitext(os.path.basename(file))[0] not in progress
        ]

        rows = pool.imap_unordered(job, files, 2 * process_count)
        for id, row, exception in tqdm(rows, total=len(files)):
            if not row:
                progress_writer.writerow((id, False, exception))
            else:
                progress_writer.writerow((id, True, None))
                writer.writerow(row)
