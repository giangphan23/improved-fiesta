import pandas as pd
import docosan_module as domo
from bs4 import BeautifulSoup
from os import listdir
from os.path import isfile, join


onlyfiles = [f for f in listdir('LC/') if isfile(join('LC/', f))]
med_ls = []
for f in onlyfiles:
    f_path = 'LC/' + f
    # print(f_path)

    with open(f_path, encoding="utf8") as html:
        soup = BeautifulSoup(html, 'html.parser')

    # list of products
    ### each element includes prod info of name, badge, dosage form (optional), ingredient (optional)
    prod_info_raw = soup.find_all('div', 'product-info txt-left')
    prod_info_ls = [prod_info_raw[i].get_text().strip() for i in range(len(prod_info_raw))]
    # len(prod_info_ls)
    prod_info = pd.Series(prod_info_ls)



    # split name & price
    prod_info2 = prod_info.str.replace('\\n\s{20}(\\n){4}', ' | ', regex=True)

    # format price
    prod_info2 = prod_info2.str.replace('\\n\s{12}\/\s', '/', regex=True)

    # split price & badge
    prod_info2 = prod_info2.str.replace('\\n\s{20}(\\n){3}', ' | ', regex=True) # cases without price
    prod_info2 = prod_info2.str.replace('\\n\s{8}(\\n){4}', ' | ', regex=True) # cases with price

    # split badge & dosage|ingredient
    prod_info2 = prod_info2.str.replace('(\\n){5}T', ' |  | T', regex=True)
    prod_info2 = prod_info2.str.replace('(\\n){5}D', ' | D', regex=True)

    # split dosage & ingredient
    prod_info2 = prod_info2.str.replace('(\\n){4}', ' | ', regex=True)

    # format dosage
    prod_info2 = prod_info2.str.replace(':(\\n){2}', ': ', regex=True)

    # format ingredient
    prod_info2 = prod_info2.str.replace('(\\n){2}', ': ', regex=True)
    prod_info2 = prod_info2.str.replace('\\n', '', regex=True)


    # test cases
    # prod_info2[17] # name, badge
    # prod_info2[10] # name, badge, ingre
    # prod_info2[33] # name, badge, dosage, ingre
    # prod_info2[30] # name, price, badge, dosage, ingre
    # print(prod_info2[17], prod_info2[10], prod_info2[33], prod_info2[30], sep='\n\n')

    # to df
    prod_info2_with_price = prod_info2[prod_info2.str.contains('(\d)??\/')]
    prod_info2_without_price = prod_info2[~prod_info2.str.contains('(\d)??\/')]

    prod_info3_with_price = prod_info2_with_price.str.split(' | ', regex=False, expand=True)
    prod_info3_with_price.columns = ['name', 'price', 'badge', 'dosage_form', 'ingredient']

    prod_info3_without_price = prod_info2_without_price.str.split(' | ', regex=False, expand=True)
    prod_info3_without_price.columns = ['name', 'badge', 'dosage_form', 'ingredient']

    # ready

    prod_info_ready = pd.concat([prod_info3_without_price, prod_info3_with_price])
    prod_info_ready.dosage_form = prod_info_ready.dosage_form.str.replace('D???ng b??o ch???: ', '')
    prod_info_ready.ingredient = prod_info_ready.ingredient.str.replace('Th??nh ph???n: ', '')

    prod_info_ready['short_name'] = prod_info_ready.name.str.replace('(Thu???c kem b??i )|(Thu???c kh??ng sinh )', '', regex=True) # remove "Thu???c kem b??i", "Thu???c kh??ng sinh"
    prod_info_ready['short_name'] = prod_info_ready['short_name'].str.replace(r'(Thu???c\s)([A-Z])', r'\2', regex=True) # remove "Thu???c" if next char is uppercase

    pat = '\s(??i???u|??i???u|??i????u|d???|ph??ng|h???|ch???|gi???m|gi??p|kh??ng vi??m|kh??ng vie??m|Tr??? Vi??m|tr??? vi??m|Tr??? Nhi???m|kh??ng virus|ch???ng ph??i nhi???m|ch???ng d??? ???ng|t??ng c?????ng|ng??n ????o th???i|ch???ng n??n|d??ng cho|b??? sung|b??? kh??|b??? th???n|b??t v???|s??t khu???n|b??? huy???t|di???t khu???n|s??t tr??ng|gi???i nhi???t|r???a v???t th????ng|c???i thi???n|b??? ph???i|cung c???p|ch???ng h??m|?????c tr???|v??? sinh|tr??? s???o|t??ng mi???n d???ch|th??ng m???t|ch???ng vi??m|tr??? ti??u ch???y|l??m d???u|b???i b???|tr??? ??au th???t ng???c|k??ch th??ch ??n ngon|chuy??n tr??? ho|Ph???c H???i|thu???c b???|tr??? nh???c m???i|tr??? thi???u vitamin|k??ch th??ch ti??u h??a|c??n b???ng h??? vi sinh|tan b???m t??m|ch???ng n???m|ng???a & b??? sung|ch???ng oxy h??a|ng??n ng???a|ti??u ?????m|t???y v?? ??i???u tr???|l??m lo??ng|ti??u nh????y|ti??u nh???y|Ti??u Nh????y|ti??u nh??y|kha??ng ti????u c????u|tr??? t??ng cholesterol|h??? tr???|d??ng g??y t??|ng??n ch???n|di???u tr???|th??c ?????y|tr??? t??ng huy???t ??p|tr??? t??ng huy???t ??p|??i???u tr??? t??ng huy???t ??p|l??m ti??u ch???t nh??y|ch???ng kh?? m???t|tr??? nhi???m khu???n|d?????ng t??m|h???i ph???c|ki???m so??t|d??ng ????? r???a|d??ng trong|tr??? t??ng|tr??? t???t|duy tr??|tr??? r???i lo???n|ng???a tai bi???n|ch???ng co th???t|b??? ph???|ch???ng ?????y h??i|ch???ng t??ng|tr??? r???i lo???n|tr??? c??c b???nh|cho tr???|r???a s???ch|d??ng r???a|???n ?????nh|Tr??? T??ng|c??c ch???ng|b???o v???|l??m m??t|l??m s???ch|Ph?? do|Gi??p H???|gia??m ??p|???u tr???|tr??? nhi???m tr??ng|kh??ng ????ng|tr??? ta??ng|ng???a say|Gi???m ??au,|ch???ng ??au|lo??ng ?????m|ch???a l???|ch???ng hoa m???t|tr??? tr??|ch???ng d??? ???ng|Gi???m Co|ng???a b???nh|tr??? ti???u ???????ng|ch???ng xu???t huy???t|ng???a nh???i|h??? s???t|ch???ng lo???n|tr??? c???m|ch???ng huy???t kh???i|ch???ng ????ng m??u|ch???ng lo|l??m gi??n|chuy??n tr???|s??c mi???ng|m??t gan|ch???ng sa s??t|h??? cholesterol|ch???ng say|thu???c l???i|),?\s.+'
    prod_info_ready['short_name'] = prod_info_ready.short_name.str.replace(pat, '', regex=True) # remove trailing words

    prod_info_ready['len'] = prod_info_ready.short_name.str.len()
    prod_info_ready = prod_info_ready[['name', 'short_name', 'len', 'badge', 'dosage_form', 'ingredient', 'price']].sort_values('len', ascending=False)

    med_ls.append(prod_info_ready)


df_final = pd.DataFrame()
for i in med_ls:
    df_final = pd.concat([df_final, i])

df_final = df_final.drop_duplicates().sort_values('len', ascending=False)
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1vVTS3QPHcBrbER_uRVjJFjsMnBhkNY-E83h_68UIfUQ/edit#gid=0', df_final)
