import pandas as pd

#exercise1

def task_number_1():
    publ = pd.read_parquet('publication.parquet', columns=['citations_num', 'date_year'])  # read columns
    auth = pd.read_parquet('author.parquet', columns=['organization_id'])
    publ_auth = pd.read_parquet('publication_author.parquet')
    org = pd.read_parquet('organization.parquet', columns=['name'])

    auth['author_id'] = auth.index + 1  # creat column as index plus one
    publ['publication_id'] = publ.index + 1
    org['organization_id'] = org.index + 1

    auth = auth[['author_id', 'organization_id']]  # swap columns
    publ = publ[['publication_id', 'citations_num', 'date_year']]
    org = org[['organization_id', 'name']]

    publ_auth_org = publ_auth.merge(auth, how='left')  # join DataFrame in one
    comp_work = publ_auth_org.merge(org, how='left')

    perm_univ = comp_work.name.unique().tolist()
    perm_univ = [x for x in perm_univ if str(x) != 'nan' and 'Perm' in x]  # complete names Perm universities

      # drop nan values
    comp_work = comp_work[comp_work.name.isin(perm_univ)]  # sort of Perm
    comp_work = comp_work.merge(publ, how='left')  # add citation info

    auth_univ = comp_work.groupby('name').nunique().author_id  # count author unique universities
    publ_univ = comp_work.groupby('name').nunique().publication_id  # count publication unique universities


      # drop nan values for count citation of publ
    citation = []  # citation for univ

    years = pd.DataFrame()
    citation_by_year = pd.DataFrame()

    for unit in comp_work.groupby('name'):  # go to sort DataFrame
        print(unit)
        name = unit[0]
        unit = pd.DataFrame(unit[1])
        years[name] = unit.groupby('date_year').nunique().publication_id
        unit.drop_duplicates(subset='publication_id', keep='first', inplace=True)

        citation_by_year[name] = unit.groupby('date_year').citations_num.sum()

       # univ = pd.DataFrame(univ[1])
       # univ.drop_duplicates(subset='publication_id', keep='first', inplace=True)
        #univ_stat = univ.groupby('date_year').nunique().publication_id

    perm_data = pd.DataFrame()

    perm_data['name'] = auth_univ.index
    perm_data['year'] = comp_work.date_year
    perm_data['auth'] = auth_univ.tolist()
    perm_data['publ_by_year'] = years
    perm_data['citation_by_year'] = citation_by_year

    print(perm_data)
'''
    import matplotlib.pyplot as plt
    import numpy as np

    # first diagram TODO:Decide naming question
    perm_data['short_name'] = ['PNIPU', 'PED', 'MED', 'PGU', 'Pharm Acd', 'Melnikov', 'Ural Branch']

    need_univ = ['Perm National Research Polytechnic University', 'Perm State National Research University',
                 'Perm State Humanitarian Pedagogical University']
    N = len(need_univ)
    need_univ = perm_data[perm_data.name.isin(need_univ)]

    index = np.arange(1, 5 * N - 1, 5)
    index1 = np.arange(2, 5 * N, 5)
    index2 = np.arange(3, 5 * N + 1, 5)

    plt.title('Bar chart of data on different universities in Perm', fontsize=20, fontname='Times New Roman')

    plt.bar(index, need_univ.publ.tolist(), color='g')
    plt.bar(index1, need_univ.citation.tolist(), color='b')
    plt.bar(index2, need_univ.auth.tolist(), color='r')

    plt.xticks(index1, need_univ.short_name.tolist(), fontsize=12, fontname='Times New Roman')
    plt.legend(['Publication', 'Citation', 'Authors'], loc=2)

    plt.savefig('Bar_Data_Perm.png', bbox_inches='tight')
    plt.close()

    # second diagram TODO:Do better view
    index3 = np.arange(1, N + 1)
    sred_cit = np.round(need_univ.citation.to_numpy() / need_univ.publ.to_numpy(), 1)

    plt.title('Average number of citations for a publication by Perm universities', fontsize=20,
              fontname='Times New Roman')
    plt.barh(index3, sred_cit, color='b')
    plt.yticks(index3, need_univ.short_name.tolist(), fontsize=14, fontname='Times New Roman')

    plt.savefig('Sred_Cit_Perm.png', bbox_inches='tight')
    plt.close()
'''

#exercise 2
import numpy as np

def task_number_2():
    publ_asjc = pd.read_parquet('publication_asjc.parquet')
    asjc_area = pd.read_parquet('asjc.parquet', columns=['code', 'subject_area'])
    publ_auth = pd.read_parquet('publication_author.parquet')
    auth_org = pd.read_parquet('author.parquet', columns=['organization_id'])
    org = pd.read_parquet('organization.parquet', columns=['name'])

    auth_org['author_id'] = auth_org.index + 1
    org['organization_id'] = org.index + 1

    asjc_area = asjc_area.rename(columns={'code': 'asjc_code'})
    publ_asjc['asjc_code'] = publ_asjc['asjc_code'].to_numpy().astype(int)

    publ_area = publ_asjc.merge(asjc_area, how='left')
    publ_area = publ_area.drop(['asjc_code'], axis=1)

    publ_org = publ_auth.merge(auth_org, how='left')
    publ_org_name = publ_org.merge(org, how='left')
    publ_org_name = publ_org_name.dropna()
    publ_name = publ_org_name.drop(['organization_id', 'author_id'], axis=1)

    publ_area_name = publ_area.merge(publ_name, how='outer')
    comp_work = publ_area_name.dropna()

    ###############################
    perm_univ = comp_work.name.unique().tolist()
    perm_univ = [x for x in perm_univ if str(x) != 'nan' and 'Perm' in x]  # complete names Perm universities
    ###############################

    perm_tabl = comp_work[comp_work.name.isin(perm_univ)]
    perm_data2 = pd.DataFrame()

    index = 0
    flag = 1

    for univ in perm_tabl.groupby('name'):  # go to sort DataFrame
        name = univ[0]
        univ = pd.DataFrame(univ[1])
        univ_stat = univ.groupby('subject_area').nunique().publication_id
        perm_data2[name] = univ_stat.tolist()
        if flag:
            index = univ_stat.index.tolist()


    print(perm_data2.columns)
    print(perm_data2)


task_number_1()
