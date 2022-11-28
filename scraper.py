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
    prod_info2_with_price = prod_info2[prod_info2.str.contains('(\d)đ\/')]
    prod_info2_without_price = prod_info2[~prod_info2.str.contains('(\d)đ\/')]

    prod_info3_with_price = prod_info2_with_price.str.split(' | ', regex=False, expand=True)
    prod_info3_with_price.columns = ['name', 'price', 'badge', 'dosage_form', 'ingredient']

    prod_info3_without_price = prod_info2_without_price.str.split(' | ', regex=False, expand=True)
    prod_info3_without_price.columns = ['name', 'badge', 'dosage_form', 'ingredient']

    # ready

    prod_info_ready = pd.concat([prod_info3_without_price, prod_info3_with_price])
    prod_info_ready.dosage_form = prod_info_ready.dosage_form.str.replace('Dạng bào chế: ', '')
    prod_info_ready.ingredient = prod_info_ready.ingredient.str.replace('Thành phần: ', '')

    prod_info_ready['short_name'] = prod_info_ready.name.str.replace('(Thuốc kem bôi )|(Thuốc kháng sinh )', '', regex=True) # remove "Thuốc kem bôi", "Thuốc kháng sinh"
    prod_info_ready['short_name'] = prod_info_ready['short_name'].str.replace(r'(Thuốc\s)([A-Z])', r'\2', regex=True) # remove "Thuốc" if next char is uppercase

    pat = '\s(điều|Điều|điều|dự|phòng|hỗ|chỉ|giảm|giúp|kháng viêm|kháng viêm|Trị Viêm|trị viêm|Trị Nhiễm|kháng virus|chống phơi nhiễm|chống dị ứng|tăng cường|ngăn đào thải|chống nôn|dùng cho|bổ sung|bổ khí|bổ thận|bát vị|sát khuẩn|bổ huyết|diệt khuẩn|sát trùng|giải nhiệt|rửa vết thương|cải thiện|bổ phổi|cung cấp|chống hăm|đặc trị|vệ sinh|trị sẹo|tăng miễn dịch|thông mật|chống viêm|trị tiêu chảy|làm dịu|bồi bổ|trị đau thắt ngực|kích thích ăn ngon|chuyên trị ho|Phục Hồi|thuốc bổ|trị nhức mỏi|trị thiếu vitamin|kích thích tiêu hóa|cân bằng hệ vi sinh|tan bầm tím|chống nấm|ngừa & bổ sung|chống oxy hóa|ngăn ngừa|tiêu đờm|tẩy và điều trị|làm loãng|tiêu nhầy|tiêu nhầy|Tiêu Nhầy|tiêu nhày|kháng tiểu cầu|trị tăng cholesterol|hổ trợ|dùng gây tê|ngăn chặn|diều trị|thúc đẩy|trị tăng huyết áp|trị tăng huyết áp|điẻu trị tăng huyết áp|làm tiêu chất nhày|chống khô mắt|trị nhiễm khuẩn|dưỡng tâm|hồi phục|kiểm soát|dùng để rửa|dùng trong|trị tăng|trị tắt|duy trì|trị rối loạn|ngừa tai biến|chống co thắt|bổ phế|chống đầy hơi|chống tăng|trị rối loạn|trị các bệnh|cho trẻ|rửa sạch|dùng rửa|ổn định|Trị Tăng|các chứng|bảo vệ|làm mát|làm sạch|Phù do|Giúp Hạ|giảm áp|ều trị|trị nhiễm trùng|kháng đông|trị tăng|ngừa say|Giảm Đau,|chống đau|loãng đờm|chữa lỵ|chống hoa mắt|trị trĩ|chống dị ứng|Giảm Co|ngừa bệnh|trị tiểu đường|chống xuất huyết|ngừa nhồi|hạ sốt|chống loạn|trị cảm|chống huyết khối|chống đông máu|chống lo|làm giãn|chuyên trị|súc miệng|mát gan|chống sa sút|hạ cholesterol|chống say|thuốc lợi|),?\s.+'
    prod_info_ready['short_name'] = prod_info_ready.short_name.str.replace(pat, '', regex=True) # remove trailing words

    prod_info_ready['len'] = prod_info_ready.short_name.str.len()
    prod_info_ready = prod_info_ready[['name', 'short_name', 'len', 'badge', 'dosage_form', 'ingredient', 'price']].sort_values('len', ascending=False)

    med_ls.append(prod_info_ready)


df_final = pd.DataFrame()
for i in med_ls:
    df_final = pd.concat([df_final, i])

df_final = df_final.drop_duplicates().sort_values('len', ascending=False)
domo.update_gsheet('https://docs.google.com/spreadsheets/d/1vVTS3QPHcBrbER_uRVjJFjsMnBhkNY-E83h_68UIfUQ/edit#gid=0', df_final)
