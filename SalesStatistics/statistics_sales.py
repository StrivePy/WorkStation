import numpy as np
import pandas as pd
import os
import re


def get_path(dir_path):
    """
    获取指定目录下的所有子文件的具体路径，并根据站点备注各自的pandas读数时的跳行数量，然后提取月份
    前的账号和站点作为pandas处理数据时的行标签。
    北美：'美国', '加拿大', '墨西哥'
    欧盟：'德国', '意大利', '法国', '英国', '西班牙'

    return：子文件信息、路径和跳行数字典
    """
    site_dict = dict()
    date = None
    nas = ['美国', '加拿大']
    eus = ['德国', '意大利', '法国', '英国', '西班牙', '墨西哥', '日本']
    for root, dirnames, filenames in os.walk(dir_path):
        for file in filenames:
            # 提取数字前的账号和站点
            pattern = re.compile('(.*?)(\d+).*?', re.DOTALL)
            result = re.search(pattern, file)
            account_site = result.group(1)
            date = result.group(2)
            # 单独提取站点
            pattern_site = re.compile('\w.*?([\u4e00-\u9fa5]+)\d+.*?', re.DOTALL)
            result_site = re.search(pattern_site, file)
            site = result_site.group(1)
            # 合并获得完整路径
            file_path = os.path.join(root, file)
            for na in nas:
                if file.find(na) != -1:
                    skp_rows = 7
                    site_dict[account_site] = dict(zip(['site', 'file_path', 'skp_rows'], [site, file_path, skp_rows]))
            for eu in eus:
                if file.find(eu) != -1:
                    skp_rows = 6
                    site_dict[account_site] = dict(zip(['site', 'file_path', 'skp_rows'], [site, file_path, skp_rows]))
    return site_dict, date


def str_to_float(df, seller_type):
    """
    筛选出售卖者的订单，不同站点，售卖者字段不同。然后将销售额和运费抵扣两列处理成浮点型数据
    然后分别计算总销售额和FBA运费
    """
    # 筛选出售卖者订单
    df = df.loc[df.iloc[:, 0] == seller_type]
    # 将字符串处理为浮点型
    if pd.api.types.is_string_dtype(df.iloc[:, 2]) == True:
        df.iloc[:, 2] = df.iloc[:, 2].str[::-1].str.replace(',', '.', 1).str[::-1].str.replace(',', '').astype(np.float)
    if pd.api.types.is_string_dtype(df.iloc[:, 3]) == True:
        df.iloc[:, 3] = df.iloc[:, 3].str[::-1].str.replace(',', '.', 1).str[::-1].str.replace(',', '').astype(np.float)
    # 计算总销售额
    origin_sales = df.iloc[:, 2:].sum().sum()
    # 计算FBA运费
    fba_cost = df.loc[df.iloc[:, 1]=='Amazon'].iloc[:, 3].sum()
    return origin_sales, fba_cost


def calculated_sales(sites):
    '''
    处理一个Excel表格，根据站点不同，读取时跳过特定行。北美跳7行(墨西哥除外)，欧盟跳6行才
    能读取到表头然后取第3(type,交易类型)、9(fullfilment,运输方式)、13(product sales,销
    售额)、14(shipping credits,运费抵扣)列。不同站命名不同，但取数列数都是相同的。

    return: 各站点销售额和FBA运费的Dataframe
    '''
    # 各站点的销售额和运费
    site_sales_fba = dict()
    for site in sites.keys():
        # 跳行读数，取特定列
        df = pd.read_excel(sites[site]['file_path'], usecols=[2, 8, 12, 13], skiprows=sites[site]['skp_rows'])
        if sites[site]['site'] in ['美国', '英国', '加拿大']:
            origin_sales, fba_cost = str_to_float(df, 'Order')
            site_sales_fba[site] = [origin_sales, fba_cost]

        if sites[site]['site'] == '墨西哥':
            origin_sales, fba_cost = str_to_float(df, 'Pedido')
            site_sales_fba[site] = [origin_sales, fba_cost]

        if sites[site]['site'] == '德国':
            origin_sales, fba_cost = str_to_float(df, 'Bestellung')
            site_sales_fba[site] = [origin_sales, fba_cost]

        if sites[site]['site'] == '法国':
            origin_sales, fba_cost = str_to_float(df, 'Commande')
            site_sales_fba[site] = [origin_sales, fba_cost]

        if sites[site]['site'] == '意大利':
            origin_sales, fba_cost = str_to_float(df, 'Ordine')
            site_sales_fba[site] = [origin_sales, fba_cost]

        if sites[site]['site'] == '西班牙':
            origin_sales, fba_cost = str_to_float(df, 'Pedido')
            site_sales_fba[site] = [origin_sales, fba_cost]

        if sites[site]['site'] == '日本':
            origin_sales, fba_cost = str_to_float(df, '注文')
            site_sales_fba[site] = [origin_sales, fba_cost]

    df_site_sales_fba = pd.DataFrame(site_sales_fba, index=['销售额原币', 'FBA配送费'])
    df_site_sales_fba = df_site_sales_fba.T
    df_site_sales_fba.index.name = '站点'
    return df_site_sales_fba


def currency_exchange(site_sales) :
    """
    计算各个站点的实际销售额原币，并将其转换为美元
    汇率（转美元）对应表：
            日元          0.0094309
            欧元          1.1097927
            墨西哥比索     0.04991
            英镑          1.2244953
            加币          0.75643
    汇率（美转人民币）：     6.8747

    return： 新增“实际销售原币”和“实际销售USD”列的site_sales
    """
    # 货币转美元映射
    conversion_rate = {'美元': 1, '日元': 0.0094309, '欧元': 1.1097927,
                       '加币': 0.75643, '比索': 0.04991, '英镑': 1.2244953, '人民币': 6.8747}
    # 计算实际销售额原币
    site_sales['实际销售额原币'] = site_sales['销售额原币'] - site_sales['FBA配送费']
    # 各个站点的实际销售额转换为美元
    site_sales.reset_index(inplace=True)
    site_sales.loc[site_sales['站点'].str.contains('美国'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['美元']
    site_sales.loc[site_sales['站点'].str.contains('加拿大'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['加币']
    site_sales.loc[site_sales['站点'].str.contains('墨西哥'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['比索']
    site_sales.loc[site_sales['站点'].str.contains('英国'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['英镑']
    site_sales.loc[site_sales['站点'].str.contains('日本'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['日元']
    site_sales.loc[site_sales['站点'].str.contains('德国'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['欧元']
    site_sales.loc[site_sales['站点'].str.contains('法国'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['欧元']
    site_sales.loc[site_sales['站点'].str.contains('意大利'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['欧元']
    site_sales.loc[site_sales['站点'].str.contains('西班牙'), '实际销售额USD'] = site_sales['实际销售额原币'] * conversion_rate['欧元']
    # 各站点的实际销售额转换为人民币
    site_sales['实际销售额RMB'] = site_sales['实际销售额USD'] * conversion_rate['人民币']
    return site_sales


def mainfunc():
    # 调用获取文件路径函数，得到站点文件详情
    site_file, date = get_path('.\\origin')
    # 处理站点文件，得到各站点销售额和FBA运费
    site_sales = calculated_sales(site_file)
    # 调用汇率转换函数，同意转换为美元和人民币
    result = currency_exchange(site_sales)
    result.set_index('站点', inplace=True)
    # 将数据导出为excel文件
    result.to_excel('.\\%s月业绩计算表格.xlsx' % date)


mainfunc()
