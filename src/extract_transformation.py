import codecs
import json
import os
from collections import defaultdict

from numpy import hstack, matrix, random
from rdflib import Graph

import_code = """
from american_transform.aat_term import *
from american_transform.aggregations import *
from american_transform.date_manipulation import *
from american_transform.html_manipulation import *
from american_transform.location_manipulation import *
from american_transform.string_manipulation import *
from american_transform.uri_manipulation import *
"""

x = None


def getValue(value):
    global x
    if value in x:
        return x[value]
    return ""


from bs4 import BeautifulSoup
import pandas as pd


def xml2df(xml_doc):
    f = codecs.open(xml_doc, 'r', encoding="utf-8")
    soup = BeautifulSoup(f)

    name_list = []
    text_list = []
    attr_list = []

    def recurs(soup):
        try:
            for j in soup.contents:
                try:
                    # print j.name
                    if j.name != None:
                        name_list.append(j.name)
                except:
                    pass
                try:
                    # print j.text
                    if j.name != None:
                        # print j.string
                        text_list.append(j.string)
                except:
                    pass
                try:
                    # print j.attrs
                    if j.name != None:
                        attr_list.append(j.attrs)
                except:
                    pass
                recurs(j)
        except:
            pass

    recurs(soup)

    attr_names_list = [q.keys() for q in attr_list]
    attr_values_list = [q.values() for q in attr_list]

    columns = hstack((hstack(name_list),
                      hstack(attr_names_list)))
    data = hstack((hstack(text_list),
                   hstack(attr_values_list)))

    df = pd.DataFrame(data=matrix(data.T), columns=columns)

    return df


if __name__ == "__main__":
    transformation_count = 0
    conditional_count = 0
    uri_count = 0
    transformation_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: [])))

    for museum_folder in os.listdir("data"):
        museum_path = os.path.join("data", museum_folder)
        for dataset_folder in os.listdir(museum_path):
            dataset_path = os.path.join(museum_path, dataset_folder)
            if not os.path.isdir(dataset_path): continue
            for file_name in os.listdir(dataset_path):
                if file_name.endswith("ttl"):
                    file_path = os.path.join(dataset_path, file_name)
                    g = Graph()
                    g.parse(file_path, format="ttl")

                    for s, p, o in g:
                        if str(p) == "http://isi.edu/integration/karma/dev#sourceName":
                            source_name = str(o)
                    for s, p, o in g:
                        if str(p) == "http://isi.edu/integration/karma/dev#hasWorksheetHistory":
                            history_str = str(o)
                            history_list = json.loads(history_str)
                            for command in history_list:
                                if command["commandName"] == "SubmitPythonTransformationCommand":
                                    transformation = {"inputColumns": []}
                                    for obj in command["inputParameters"]:
                                        if obj["name"] == "inputColumns":
                                            for column in json.loads(obj["value"]):
                                                transformation["inputColumns"].append(column["value"][0]["columnName"])
                                        elif obj["name"] == "outputColumns":
                                            for column in json.loads(obj["value"]):
                                                transformation["outputColumn"] = column["value"][0]["columnName"]
                                        elif obj["name"] == "transformationCode":
                                            transformation["code"] = obj["value"]
                                    transformation_count += 1
                                    if "if" in transformation["code"]:
                                        conditional_count += 1
                                    if "URI" in transformation["outputColumn"]:
                                        uri_count += 1
                                    print(museum_folder, dataset_folder, transformation)
                                    transformation_dict[museum_folder][dataset_folder][source_name].append(
                                        transformation)

    print(transformation_count)
    print(conditional_count)
    print(uri_count)
    error_count = 0
    inout_list = []

    for museum in transformation_dict:
        for dataset in transformation_dict[museum]:
            folder_path = "data/%s/%s" % (museum, dataset)
            for file_name in transformation_dict[museum][dataset]:
                if file_name.endswith(".csv"):
                    try:
                        df = pd.read_csv(os.path.join(folder_path, file_name), dtype=str).fillna("")
                        # if False in df.columns.values:
                        #     del df[False]
                    except Exception as e:
                        print(e)
                        continue
                else:
                    continue
                # df[df is None] = ""
                # print(transformation_dict[museum][dataset])
                for transformation in transformation_dict[museum][dataset][file_name]:
                    inout_pair = {}
                    print(transformation["code"])
                    transformation["code"] = "def transform(a):\n\tglobal x\n\tx = a\n\t" + \
                                             "\n\t".join(transformation["code"].split("\n"))
                    # print(import_code + transformation["code"])
                    # print(museum, dataset, file_name)
                    # print(transformation["code"])
                    # print(df.columns.values)
                    exec (import_code + transformation["code"])
                    # print("output:", transformation["outputColumn"])
                    df[transformation["outputColumn"]] = df.apply(lambda a: eval("transform(a)"), axis=1)
                    inout_pair["output"] = df[transformation["outputColumn"]].values.tolist()
                    inout_pair["input"] = [" ".join([str(y) for y in x]) for x in df[transformation[
                        "inputColumns"]].values.tolist()]
                    inout_pair["code"] = transformation["code"]
                    inout_list.append(inout_pair)
                    error_count += 1

    file_id = 0
    print(error_count)

    if not os.path.exists("test_data/raw"):
        os.makedirs("test_data/raw")
    if not os.path.exists("test_data/transformed"):
        os.makedirs("test_data/transformed")
    if not os.path.exists("test_data/code"):
        os.makedirs("test_data/code")
    if not os.path.exists("test_data/groundtruth"):
        os.makedirs("test_data/groundtruth")

    with open("sample_data.txt", "w") as s_writer:

        for inout_pair in inout_list:
            print(inout_pair["code"])
            if "if" in inout_pair["code"]:
                continue

            # if "uri_from_fields" not in inout_pair["code"]:
            #     continue


            in_list = inout_pair["input"]
            out_list = inout_pair["output"]

            for i in range(5):
                index = random.choice(len(in_list))
                s_writer.write('"%s","%s"\n' % (in_list[index], out_list[index]))

            s_writer.write("---------------------------------------------------------------------------------------\n")


            assert len(in_list) == len(out_list)
            if len(in_list) > 400:
                size = 200
            else:
                size = len(in_list) // 2
            train_indices = random.choice(range(len(in_list)), size, replace=False)

            train_in_list = []
            train_out_list = []

            for indx in train_indices:
                train_out_list.append(out_list[indx])

            for indx in sorted(train_indices, reverse=True):
                print(indx, len(in_list), len(out_list))
                del in_list[indx]
                del out_list[indx]

            if in_list == out_list:
                continue
            with codecs.open("test_data/raw/%s.csv" % file_id, "w", encoding="utf-8") as writer:
                if inout_pair["input"]:
                    writer.write("\n".join(['"%s"' % str(x).replace('"', ",") for x in in_list]))
                else:
                    writer.write("")
            with open("test_data/transformed/%s.csv" % file_id, "w", encoding="utf-8") as writer:
                if inout_pair["output"]:
                    writer.write("\n".join(['"%s"' % str(x).replace('"', ",") for x in train_out_list]))
                else:
                    writer.write("")

            with open("test_data/groundtruth/%s.csv" % file_id, "w", encoding="utf-8") as writer:
                if inout_pair["output"]:
                    writer.write("\n".join(['"%s"' % str(x).replace('"', ",") for x in out_list]))
                else:
                    writer.write("")

            with open("test_data/code/%s.csv" % file_id, "w", encoding="utf-8") as writer:
                if inout_pair["code"]:
                    writer.write(inout_pair["code"])
                else:
                    writer.write("")
            file_id += 1
