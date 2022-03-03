from collections import defaultdict
from decimal import getcontext
import pandas as pd
import json
import os
from huggingface_hub import list_datasets


def get_dataset():
    """
    由于该CSV的数据有两个来源，这里通过huggingface包获取所有的datasets信息，并转换成字典;
    主要使用: creatorName、isPrivate、licenseName、title
    :return hashmap
    """
    all_datasets = list_datasets()
    # print(f"Number of datasets on Hub: {len(all_datasets)}")

    hashmap = {}

    for dataset in all_datasets:
        hashmap[dataset.id] = dataset

    return hashmap


hashmap = get_dataset()


def get_content(citation, attr):
    """
    :param attr 从dataset.citation中截取指定内容
    :return "" 或 str
    """
    if not citation:  # 有的citation会为空，防止异常
        return ""

    left_index = citation.find(attr)

    if left_index != -1:  # 如果该citation中存在attr这个属性
        for i in range(left_index, len(citation)):
            left_index = i  # attr左边界
            if citation[i] == '{' or citation[i] == '"':
                break

        right_index = left_index
        for i in range(left_index + 1, len(citation)):
            if citation[i] == '}' or citation[i] == '"':  # 结束定界符出现
                right_index = i  # attr右边界
                break

        # attr中存在换行，将其替换为空格; attr中可能有多个空格连续，将多个替换为一个空格
        return ' '.join(citation[left_index + 1: right_index].replace('\n', ' ').split())
    else:
        return ""


# def get_right_index(citation, left_index):
#     cols = ['title', 'author', 'Author', 'booktitle', 'month', 'year', 'address', 'publisher', 'url', 'pages', 'abstract', 'journal', 'volume', 'number', 'editor', 'Proceedings']
#     index = len(citation) + 1
#     for col in cols:
#         new_index = citation.find(col)
#         if new_index > left_index and new_index < index:
#             index = new_index

#     return index


def get_creator(dataset):
    """
    获取作者信息，如果数据集中有则直接返回，如果没有则到citation中截取字符串
    :param dataset: 类型为DatasetInfo，通过huggingface包获取
    :return "" 或 str
    """
    if dataset.author:
        return dataset.author

    citation = dataset.citation

    return get_content(citation, "author")


def get_title(citation):
    """
    通过dataset.citation截取title相关字符串
    citation: dataset.citation
    :return "" 或 str
    """
    return get_content(citation, "title")


def get_license(tags):
    """
    获取该dataset的license
    tags: dataset.tags
    :return "unknown" 或 str
    """
    for tag in tags:
        key_value = tag.split(":")
        if len(key_value) < 2:  # 数据集不规范，防止异常
            continue
        if key_value[0] == "licenses":
            return key_value[1]
    return "unknown"


df = pd.DataFrame(columns=['id', 'ref', 'subtitle', 'creatorName', 'creatorUrl', 'totalBytes', 'url', 'lastUpdated', 'downloadCount', 'isPrivate',
                  'isFeatured', 'licenseName', 'description', 'ownerName', 'ownerRef', 'kernelCount', 'title', 'currentVersionNumber', 'usabilityRating'])

# 内容未commit，资源链接为: https://github.com/ZichengQu/datasets/tree/master/datasets
for root, dirs, files in os.walk(r"src/datasets"):
    for file in files:
        path = os.path.join(root, file)
        if str(file) == "dataset_infos.json":
            id = ""
            ref = root[13:]
            subtitle = ""
            creatorName = get_creator(hashmap[ref])
            creatorUrl = ""
            totalBytes = 0
            url = "https://huggingface.co/datasets/" + ref
            lastUpdated = hashmap[ref].lastModified
            downloadCount = ""
            isPrivate = hashmap[ref].private
            isFeatured = ""
            licenseName = get_license(hashmap[ref].tags)
            description = ""  # hashmap[ref].description, huggingface中存在该属性
            ownerName = creatorName  # 等价于creatorName
            ownerRef = creatorUrl  # 等价于creatorUrl
            kernelCount = ""
            title = get_title(hashmap[ref].citation)
            currentVersionNumber = ""
            usabilityRating = ""

            with open(os.path.join(root, file), 'r', encoding='UTF-8') as f:
                load_dict = json.load(f)

                for key, items in load_dict.items():  # 一个数据集中可能有多个子集，获取sum(子集的size)
                    totalBytes += items['dataset_size']

            new_df = pd.DataFrame([id, ref, subtitle, creatorName, creatorUrl, totalBytes, url, lastUpdated, downloadCount, isPrivate,
                                  isFeatured, licenseName, description, ownerName, ownerRef, kernelCount, title, currentVersionNumber, usabilityRating]).T
            new_df.columns = df.columns

            df = pd.concat([df, new_df], ignore_index=True)
        break


df.to_csv("dataset.csv", index=False)  # 输出到csv
