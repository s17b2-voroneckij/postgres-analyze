import psycopg2
import psycopg2.extensions


def get_root_page_level(c: psycopg2.extensions.cursor, idx: str):
    c.execute(f"SELECT root, level FROM bt_metap('{idx}')")
    return c.fetchall()[0]

def inspect_index(c: psycopg2.extensions.cursor, idx):
    root, level = get_root_page_level(c, idx)
    return recursive_inspect(c, idx, root, level)


def parse_int(s: str):
    if len(s) == 0:
        return -1000
    result = 0
    s = s.split(' ')[::-1]
    for ds in s:
        result <<= 8
        result += int("0x" + ds, 16)
    return result

list_pages = []

def recursive_inspect(c: psycopg2.extensions.cursor, idx: str, root: int, steps: int):
    result = {}
    c.execute(f"SELECT itemoffset, ctid, data FROM bt_page_items('{idx}', {root})")
    page_contents = c.fetchall()
    for i in range(len(page_contents)):
        page_contents[i] = list(page_contents[i])
        page_contents[i][2] = parse_int(page_contents[i][2])
        page_contents[i][1] = eval(page_contents[i][1])[0]
    if steps == 0:
        curr = list(map(lambda t: t[2], page_contents))
        list_pages.append(curr)
        return curr
    else:
        for page_entry in page_contents:
            result[page_entry[0]] = (recursive_inspect(c, idx, page_entry[1], steps - 1))
    return result


conn = psycopg2.connect(dbname='market', user='server', password='123456', host='localhost')
c = conn.cursor()
inspection = inspect_index(c, "ids_pkey")
for list_page in list_pages:
    print(len(list_page), min(list_page), max(list_page))

a = 0
