import multiprocessing
import os

from bs4 import BeautifulSoup
from tqdm import tqdm


def extract_one(xml_string):
    p_text_list = []
    soup = BeautifulSoup(xml_string, "lxml")
    body_paragraphs = soup.select("body sec > title ~ p")
    doi = soup.select_one('article-id[pub-id-type="doi"]').text
    for p in body_paragraphs:
        p_text = p.get_text()
        p_text = p_text.replace("\n", "") + "\n\n"
        p_text_list.append(p_text)
    return doi, p_text_list


def job(inputs):
    filepath, out_dir = inputs
    filename = os.path.basename(filepath)
    out_path = os.path.join(out_dir, os.path.splitext(filename)[0] + ".txt")

    with open(filepath, "r") as f, open(out_path, "w") as o_f:
        xml_string = f.read()
        doi, paragraphs = extract_one(xml_string)
        o_f.writelines(paragraphs)

    return f"{filename},{doi}\n"


def extract_all(data_dir, output_dir, meta_output_file="plosone_filelist.csv"):
    os.makedirs(output_dir, exist_ok=True)
    files = [
        os.path.join(dir, file) 
        for dir, _, files in os.walk(data_dir) 
        for file in files 
        if file.endswith(".xml") and not os.path.exists(file)
    ]

    inputs = [(file, output_dir) for file in files]
    with multiprocessing.Pool(multiprocessing.cpu_count() * 2) as pool:
        results = pool.imap(job, inputs, multiprocessing.cpu_count() * 4)

        with open(meta_output_file, "w+") as meta_f:
            for result in tqdm(results, total=len(files)):
                meta_f.write(result)


if __name__ == "__main__":
    extract_all("../allofplos_xml", "data/plos/fulltext", "data/plos/plosone_filelist.csv")
