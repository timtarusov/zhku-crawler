import scrapy
import json
import pandas as pd
from datetime import date

#source: http://www.xn--f1aismi.xn--p1ai/gkh-laws/mos1038_14.html
zhku_norm_per_capita = {
    "водоотведение и очистка сточных вод":9.12,
    "водоснабжение": 11.68,
    "обращение с тко": 0,
    "тепловая энергия": 0.0283*18 + 0.294, #подогрев помещений на 18 кв.м/чел + подогрев воды на чел.
    "газ": 13/1000,
    "электроэнергия": 45
}

COL_AGREEMENT_MLTP = 1.6

class ZhkuSpider(scrapy.Spider):
    name = 'zhku'
    
    def start_requests(self):
        urls = [
            "https://tekvo.gov35.ru/vedomstvennaya-informatsiya/info-for-citizens/tarify/tarify-na-zhku/index.php?Rajon=1973&MO=19730000&Group=1&Period=0"
        ]
        
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
            
    def parse(self, response):
        dump_dict = dict()
        column_map = {
            0: "n",
            1: "name",
            2: "date",
            3: "tarif_population",
            4: "tarif_other"
        }
        all_tables = response.css('table.zhku')
        for i,table in enumerate(all_tables):
            table_name = table.xpath('preceding-sibling::h2/text()').extract()[i]
            print(table_name)
            dump_dict[table_name] = []
            for j, row in enumerate(table.xpath('tr')):
                if j > 1: #skip first 2 lines
                    tds = row.xpath('td').xpath('text()').extract()
                    row_dict = {column_map[k]:value for k, value in enumerate(tds)}
                    dump_dict[table_name].append(row_dict)
                    
                        
            
        cost_list = []
        
        def populate_cost_list(resource, max_date, cost, norm, mtlp=COL_AGREEMENT_MLTP):
            if (4<date.today().month<9) and (resource == "тепловая энергия"): #июнь-сентябрь включительно
                cost_list.append({
                    "ресурс":resource,
                    "дата": max_date,
                    "тариф": cost,
                    "норматив на человека": norm,
                    "колл договор множитель": mtlp,
                    "итого жкх": 0                   
                })
            else:
                try:
                    total = cost * norm * mtlp
                except TypeError:
                    cost = float("".join(cost)) #если разделитель по ошибке - запятая  
                    total = cost * norm * mtlp
                cost_list.append({
                    "ресурс":resource,
                    "дата": max_date,
                    "тариф": cost,
                    "норматив на человека": norm,
                    "колл договор множитель": mtlp,
                    "итого жкх": total                   
                })
                   
        # with open('./data/data.json', 'w', encoding='utf-8') as f:
        #     json.dump(dump_dict, f, indent=4, ensure_ascii=False)
        for k, v in dump_dict.items():
            df = pd.DataFrame(v)
            if k.lower() ==  "электроэнергетика и газ":
                gas_df = df[(df["name"].str.contains("газ"))]
                gas_df = gas_df[~gas_df["name"].str.contains("сжижен")]
                electr_df = df[df.name.str.contains("электроэнер")]
                # gas_df.to_excel(f"./data/газ.xlsx", index=False)
                # electr_df.to_excel(f"./data/электроэнергия.xlsx", index=False)
                
                gas_cost = gas_df.tarif_population.max() #для жилых помещений максимальная стоимость
                gas_date = pd.to_datetime(gas_df.date).max()
                populate_cost_list("газ", gas_date, gas_cost, zhku_norm_per_capita["газ"])
                
                el_cost = electr_df.tarif_population.median() #берем медианное значение
                el_date = pd.to_datetime(electr_df.date).max()
                populate_cost_list("электроэнергия", el_date, el_cost, zhku_norm_per_capita["электроэнергия"])
                
                
            else:
                res_cost = df.tarif_population.max()
                res_date = pd.to_datetime(df.date).max()
                populate_cost_list(k.lower(), res_date, res_cost, zhku_norm_per_capita[k.lower()]) 
                # df.to_excel(f"./data/{k.lower()}.xlsx", index=False)
        
        # cost_df = pd.DataFrame(cost_list)
        # cost_df.to_excel(f"./data/общая_стоимость.xlsx", index=False)   
        
        class Resource(scrapy.Item):
            res = scrapy.Field()
            date = scrapy.Field(serializer=str)
            tarif = scrapy.Field()
            norm = scrapy.Field()
            mltp = scrapy.Field()
            total = scrapy.Field()
            
        class Cost(scrapy.Item):
            res = scrapy.Field()
        
        cost = Cost()
        cost["res"] = []
        for res in cost_list:

            item = Resource()
            item["res"] = res["ресурс"]
            item["date"] = res["дата"]
            item["tarif"] = res["тариф"]
            item["norm"] = res["норматив на человека"]
            item["mltp"] = res["колл договор множитель"]
            item["total"] = res["итого жкх"]
            cost["res"].append(dict(item))    
            # yield item
        return cost    