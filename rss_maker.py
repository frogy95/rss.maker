# To Do
# 1. Looger 달기, 에러처리
# 3. 서비스 하기
# 4. paging 처리하기
# 5. 다른 url들도 처리할 수 있도록 확장성 높이기
# 6. GitHub에 올리기
import sqlite3
import PyRSS2Gen
from urllib.request import urlopen
from bs4 import BeautifulSoup

BASEPATH = 'c:\\test\\'


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class DbHandler:

    def __init__(self):
        self.conn = sqlite3.connect("hmcdb.db")
        self.conn.row_factory = dict_factory
        self.create_table()

    def insert(self, article):
        cur = self.conn.cursor()
        sql = """Insert Into rss(title ,link, description, 
        item_title, item_link, item_description, item_author, item_category, item_pubDate, item_guid)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        ret = cur.execute(sql, (article.title, article.link, article.description,
                                article.item_title, article.item_link, article.item_description,
                                article.item_author, article.item_category, article.item_pubDate, article.item_guid))
        self.conn.commit()
        print(ret)

    def get_max_id(self, title):
        cur = self.conn.cursor()
        cur.execute("select item_guid from rss where title = '{0}' order by id desc limit 1;".format(title))
        rows = cur.fetchall()
        if len(rows) > 0:
            max_id = int(rows[0]['item_guid'])
        else:
            max_id = 0
        return max_id

    def create_table(self):
        sql = """ CREATE TABLE IF NOT EXISTS rss (
                    id integer PRIMARY KEY AUTOINCREMENT,
                    
                    title text NOT NULL,
                    link text NOT NULL,
                    description text NOT NULL,
                    
                    item_title text NOT NULL,
                    item_link text NOT NULL,
                    item_description text NOT NULL,
                    item_author text NOT NULL,
                    item_category text NOT NULL,
                    item_pubDate text NOT NULL,
                    item_guid text NOT NULL
                ); """
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()


class Issues:
    title = ""
    link = ""
    description = ""
    item_title = ""
    item_link = ""
    item_description = ""
    item_author = ""
    item_category = ""
    item_pubDate = ""
    item_guid = ""


class IssuesEpeople(Issues):

    def __init__(self, _link):
        self.title = "전자공청회"
        self.link = "https://www.epeople.go.kr/jsp/user/frame/po/policy/UPoFrPolicyList.jsp?anc_code=1352000&amp;" \
                    + "channel=1352000&amp;menu_code=PO002"
        self.description = "보건복지부 입법/행정예고 전자공청회"
        main_content = _link[2].find('a')
        self.item_title = main_content.get('title')
        self.item_link = "https://www.epeople.go.kr/jsp/user/frame/po/policy/UPoFrPolicyView.paid?" \
                         + "callKey=I&app_no_c={0}".format(main_content.get('onclick')[8:-3])
        self.item_description = "기간 {0}".format(_link[4].text)
        self.item_author = _link[3].text.strip('\r').strip('\n').strip('\t').strip()
        self.item_category = ""
        self.item_pubDate = ""
        self.item_guid = _link[0].text
        return


def get_new_articles(db, title):
    url = "https://www.epeople.go.kr/jsp/user/frame/po/policy/UPoFrPolicyList.jsp?" \
          + "anc_code=1352000&amp;channel=1352000&amp;menu_code=PO002"
    html = urlopen(url)
    bs_object = BeautifulSoup(html, "html.parser")

    table = bs_object.body.find('table', summary="전자공청회 목록 - 번호,진행상태,제목,발제자,기간")
    rows = table.find_all('tr')
    max_id = db.get_max_id(title)
    print(max_id)

    articles = ()

    for row in reversed(rows):
        cols = row.find_all('td')
        if len(cols) == 0:
            continue

        issue = IssuesEpeople(cols)

        if issue.item_guid != "":
            articles += (issue,)

    return filter(lambda x: int(x.item_guid) > max_id, articles)


def publish_rss(db, title):
    cur = db.conn.cursor()

    cur.execute("select * from rss where title = '{0}' order by id desc limit 20;".format(title))

    rss = PyRSS2Gen.RSS2(title='', link='', description='', items=[])

    for row in cur.fetchall():
        if rss.title == "":
            rss.title = row['title']
            rss.link = row['link']
            rss.description = row['description']

        item = PyRSS2Gen.RSSItem(
            title=row['item_title'],
            link=row['item_link'],
            description=row['item_description'],
            author=row['item_author'],
            categories=row['item_category'],
            pubDate=row['item_pubDate'],
            guid=row['item_guid']
        )

        rss.items.append(item)

    rss.write_xml(open(BASEPATH + 'rss_{0}.rss'.format(title), 'w'), encoding='UTF-8')


def save_crawling_epeople(db):
    new_articles = get_new_articles(db, '전자공청회')

    return new_articles


def make_rss():
    db = DbHandler()

    new_articles = ()
    new_articles += (save_crawling_epeople(db), )

    for articles in new_articles:
        for article in articles:
            db.insert(article)

    publish_rss(db, '전자공청회')
    db.conn.close()


if __name__ == "__main__":
    make_rss()
    quit()