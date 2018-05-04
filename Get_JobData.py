# -*- coding: utf-8 -*-
"""
Created on Tue Dec 12 21:18:41 2017

@author: Yan
"""
import time
import requests
from bs4 import BeautifulSoup
import json
import numpy as np
import pandas as pd

def SearchJob(page=1 ,job = '数据分析'):
    try:        
        url = 'https://www.lagou.com/jobs/positionAjax.json?city=%E5%B9%BF%E5%B7%9E&needAddtionalResult=false&isSchoolJob=0'
        headers = {'Host': 'www.lagou.com'
                    ,'Connection': 'keep-alive'
                    ,'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36'
                    ,'Referer': 'https://www.lagou.com/jobs/list_%E6%95%B0%E6%8D%AE%E5%88%86%E6%9E%90?labelWords=&fromSearch=true&suginput='
                }
        if page == 1:
            data = {'first':'true', 'pn':'1', 'kd':job}
        else:
            data = {'first':'false', 'pn':str(page), 'kd':job}
        r = requests.post(url, headers=headers, data=data)
        r.raise_for_status()
        r.encoding = 'utf-8'
        return r.text
    except:
        print("网络访问出错")		
        return "网络访问出错"

def Get_Search_Result(js):#获取每页结果
    JobResult = []
    PageSize = len(js['content']['hrInfoMap'])
    for Result in range(PageSize):
        Company_name = js['content']['positionResult']['result'][Result]['companyShortName']
        Position_Name = js['content']['positionResult']['result'][Result]['positionName']
        Position_Id = js['content']['positionResult']['result'][Result]['positionId']
        Salary = js['content']['positionResult']['result'][Result]['salary']
        Salary0 = Salary.split('-') #分割最高、最低工资
        Sal1 = int(Salary0[0].lower().split('k')[0]) #小写化、并去除K
        Sal2 = int(Salary0[1].lower().split('k')[0])
        Sal_Avg = (Sal1 + Sal2)/2 #计算平均值
        WorkYear = js['content']['positionResult']['result'][Result]['workYear']
        OneJobResult = [Company_name, Position_Name, Position_Id, Salary, Sal1, Sal2, Sal_Avg, WorkYear]
        JobResult.append(OneJobResult)
    return JobResult

def Get_JobList(job = '数据分析'):  #整合结果为列表
    content = SearchJob(1,job) #获取第一页
    js = json.loads(content)
    ResultSize = js['content']['positionResult']['resultSize'] #每页条目
    PageNum = int(np.ceil(js['content']['positionResult']['totalCount']/ResultSize)) #计算总页数
    
    JobLists = []
    JobList = []
    JobList = Get_Search_Result(js) #获取第一页列表
    JobLists.extend(JobList)
    for Page in range(PageNum-1): #获取第二页及以后页列表
        taketime = np.random.uniform(11, 16)
        time.sleep(taketime)
        content = SearchJob(Page+2,job) #从第二页（2）开始
        js = json.loads(content)
        JobList = Get_Search_Result(js)
        print(Page+2) #计数
        JobLists.extend(JobList)
    return JobLists

def Get_Description(JobID): #获取详细职位描述
    try:        
        url = 'https://www.lagou.com/jobs/' + JobID + '.html'
        headers = {'Host': 'www.lagou.com'
                    ,'Connection': 'keep-alive'
                    ,'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.75 Safari/537.36'
                }
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        r.encoding = 'utf-8'
    except:
        print(JobID + "网络访问出错")
    
    try:
        soup = BeautifulSoup(r.text, "html.parser")
        Doc = soup.find('dl', attrs = {'class' : 'job_detail'})    
        Doc_lines = Doc.findAll('p')
        Description = ''
        for line in Doc_lines:
            if line.string is not None:    #判定是否为空,跳过子类
                Description += line.string
            else: #解析子类
                for child in line.children:
                    if child.string is not None: #跳过标签
                        Description += child.string
                    else:
                        for child2 in child.children:
                            if child2.string is not None:
                                Description += child2.string
    except:
        Description = '解析出错'
    print(Description)
    return Description



def Merge_Description(JobLists):
    for i,JobID in enumerate(JobLists):
        taketime = np.random.uniform(12, 16)
        time.sleep(taketime)
        Description = Get_Description(str(JobID[2]))
        JobLists[i].append(Description)
        print(i)
    return JobLists

def Fix_Description(JobLists): #修复因网络问题而导致抓取失败的行
    for Job in JobLists:
        if Job[-1] == '解析出错':
            taketime = np.random.uniform(11, 16)
            time.sleep(taketime)
            Job[-1] = Get_Description(str(Job[2]))
            
JobLists = Get_JobList(job = '数据分析')
JobLists_New = Merge_Description(JobLists)
Fix_Description(JobLists_New)
df_Job_List = pd.DataFrame(JobLists_New, columns=['C_Name', 'P_Name', 'P_id','Sal','Sal1','Sal2','Sal_Avg','Work_Year', 'Description'])
df_Job_List.to_csv('Data/JobRaw.csv', index=False)