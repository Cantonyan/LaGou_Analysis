# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 21:18:41 2017

@author: Yan
"""
import pandas as pd
import jieba

def Judge_Skill(DocList): #遍历技能，转换为数组
    WordList = ['python' ,'sql' ,'mysql', 'hadoop', 'excel' ,'r' ,'spss' ,'sas' ,'matlab','java', '分类', '聚类', '关联', '回归']    
    ReturnVec = [0]*len(WordList)
    for word in DocList:
        if word.lower() in WordList:#改为小写遍历
            ReturnVec[WordList.index(word.lower())] = 1 #存入列表
    if ReturnVec[1] or ReturnVec[2]:#判断是否存在SQL
        ReturnVec[1] = 1
    del ReturnVec[2] #删除重复SQL列
    return ReturnVec

def CleanData_Log(df): #标准化工作经验的长度、转换薪酬单位，遍历技能
    df_dum = pd.get_dummies(df['Work_Year'])
    df['Sal_Avg'] = df['Sal_Avg']*1000 #将千元转换成元
    df['Sal_Avg'] = df['Sal_Avg'].astype(int) #转换成整型
   
    Lists = []
    for i in range(df.shape[0]):
        TagSet = set([]) #初始化
        seg_list = list(jieba.cut_for_search(df.iloc[i,8])) #每行进行分词
        TagSet = TagSet | set(seg_list) #每行去重，再合并进TagSet
        DocList = list(TagSet)
        List = Judge_Skill(DocList)
        Lists.append(List)
    df_log = pd.DataFrame(Lists, columns=['Python' ,'SQL' , 'Hadoop', 'Excel' ,'R' ,'SPSS' ,'SAS' ,'Matlab','Java', '分类', '聚类', '关联', '回归'])
    df_log = df_log.join(df_dum)
    df_log = df_log.join(df['Sal_Avg'])
    return df_log

df = pd.read_csv('Data/JobRaw.csv')
df_log = CleanData_Log(df)
df_log.to_csv('Data/JobList.csv', index=False)