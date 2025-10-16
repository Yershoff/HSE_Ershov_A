#!/usr/bin/env python3
import os
import json
import requests
from datetime import datetime, date, timedelta
from decimal import Decimal
from bs4 import BeautifulSoup
import time
from utils import format_price


class ParserCBRF:
    BASE_URL = "https://www.cbr.ru/hd_base/metall/metall_base_new/"

    def __init__(self):
        self.data: dict[str, dict[str, Decimal]] = {}

    # ---------- сетевой слой ----------
    def _get_soup(self, from_date: str, to_date: str):
        params = {
            "UniDbQuery.Posted": "True",
            "UniDbQuery.From": from_date,
            "UniDbQuery.To": to_date,
            "UniDbQuery.Gold": "true",
            "UniDbQuery.Silver": "true",
            "UniDbQuery.Platinum": "true",
            "UniDbQuery.Palladium": "true",
            "UniDbQuery.so": "1",
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        for attempt in range(3):
            try:
                resp = requests.get(self.BASE_URL, params=params,
                                    headers=headers, timeout=30)
                if resp.status_code == 200:
                    resp.encoding = "utf-8"
                    return BeautifulSoup(resp.text, "html.parser")
                print(f"HTTP {resp.status_code}, retry…")
            except Exception as e:
                print(f"Request error: {e}")
            time.sleep(2)
        return None

    # ---------- парсинг одного куска ----------
    def _parse_chunk(self, soup) -> dict[str, dict[str, Decimal]]:
        table = soup.find("table", {"class": "data"})
        chunk = {}
        if not table:
            return chunk
        for row in table.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) != 5:
                continue
            date_str = cells[0].text.strip()
            try:
                d = datetime.strptime(date_str, "%d.%m.%Y").date()
                iso = d.isoformat()
                prices = {
                    "gold":     Decimal(cells[1].text.strip().replace(" ", "").replace(",", ".")),
                    "silver":   Decimal(cells[2].text.strip().replace(" ", "").replace(",", ".")),
                    "platinum": Decimal(cells[3].text.strip().replace(" ", "").replace(",", ".")),
                    "palladium":Decimal(cells[4].text.strip().replace(" ", "").replace(",", ".")),
                }
                chunk[iso] = prices
                print(f"{date_str}  Золото:{format_price(prices['gold'])}  "
                      f"Серебро:{format_price(prices['silver'])}  "
                      f"Платина:{format_price(prices['platinum'])}  "
                      f"Палладий:{format_price(prices['palladium'])}")
            except Exception:
                continue
        return chunk

    # ---------- разбиение на кварталы ----------
    @staticmethod
    def _quarter_ranges(start: date, end: date):
        ranges = []
        curr = start
        while curr <= end:
            q_end = (curr + timedelta(days=89))
            if q_end > end:
                q_end = end
            ranges.append((curr.strftime("%d.%m.%Y"),
                           q_end.strftime("%d.%m.%Y")))
            curr = q_end + timedelta(days=1)
        return ranges

    # ---------- запуск ----------
    def start(self):
        start_date = date(2008, 7, 1)
        end_date   = date.today()
        print(f"Сбор данных с {start_date} по {end_date}, "
              f"разбито на кварталы…")

        ranges = self._quarter_ranges(start_date, end_date)
        raw = {}
        for f_str, t_str in ranges:
            print(f"\nЗапрос {f_str} – {t_str}")
            soup = self._get_soup(f_str, t_str)
            if soup:
                raw.update(self._parse_chunk(soup))
            else:
                print("  пропущен (нет ответа)")
            time.sleep(0.5)

        if not raw:
            print("Не удалось получить ни одной записи")
            return False

        original_count = len(raw)
        self.data, filled_dates = self._fill_data_gaps(raw)
        filled_count = len(self.data)

        if filled_dates:
            print(f"\nЗаполнено {len(filled_dates)} нерабочих дней "
                  f"(первые 10): {sorted(filled_dates)[:10]}")
        print(f"\nИсходных записей: {original_count}")
        print(f"Итоговых  записей: {filled_count}")
        self._save_to_json()
        return True

    # ---------- заполнение пропусков ----------
    def _fill_data_gaps(self, raw: dict):
        if not raw:
            return {}, set()
        sorted_dates = sorted(raw.keys())
        start = datetime.fromisoformat(sorted_dates[0]).date()
        end   = datetime.fromisoformat(sorted_dates[-1]).date()

        filled = {}
        current = {}
        filled_dates = set()

        d = start
        while d <= end:
            iso = d.isoformat()
            if iso in raw:
                current = raw[iso]
                filled[iso] = current
            else:
                filled[iso] = current.copy()
                if current:
                    filled_dates.add(iso)
            d += timedelta(days=1)
        return filled, filled_dates

    # ---------- JSON ----------
    def _save_to_json(self):
        os.makedirs("parsed_data", exist_ok=True)
        path = os.path.join("parsed_data", "metal_prices.json")
        out = {d: {m: str(p) for m, p in pr.items()}
               for d, pr in self.data.items()}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=4, ensure_ascii=False)
        print(f"\nСохранено {len(out)} записей в {path}")


# --------------------------------------------------
# работа с сохранённым JSON
# --------------------------------------------------
class MetalPricesCBRF:
    METAL_NAMES = {
        "gold": "Золото",
        "silver": "Серебро",
        "platinum": "Платина",
        "palladium": "Палладий",
    }

    def __init__(self):
        self.data: dict[str, dict[str, Decimal]] = {}
        self._load()

    def _load(self):
        path = os.path.join("parsed_data", "metal_prices.json")
        if not os.path.exists(path):
            raise FileNotFoundError("Файл не найден. Сначала запустите парсер.")
        with open(path, encoding="utf-8") as f:
            raw = json.load(f)
        self.data = {d: {m: Decimal(p) for m, p in pr.items()}
                     for d, pr in raw.items()}
        dates = sorted(self.data)
        print(f"Загружены данные: {dates[0]} … {dates[-1]}  ({len(dates)} записей)")

    # ---------- публичные методы ----------
    def prices_by_date(self, iso_date: str):
        pr = self.data.get(iso_date)
        return {m: format_price(p) for m, p in pr.items()} if pr else None

    def prices_last(self):
        if not self.data:
            return None, None
        last = sorted(self.data)[-1]
        return {m: format_price(p) for m, p in self.data[last].items()}, last

    def prices_range(self, d_from: str, d_to: str):
        out = []
        for d, prices in sorted(self.data.items()):
            if d_from <= d <= d_to:
                out.append((d, {m: format_price(p) for m, p in prices.items()}))
        return out

    def display_name(self, metal):
        return self.METAL_NAMES.get(metal, metal)


# --------------------------------------------------
# консольное меню
# --------------------------------------------------
def _input_date(prompt):
    while True:
        txt = input(prompt).strip()
        try:
            datetime.strptime(txt, "%Y-%m-%d")
            return txt
        except ValueError:
            print("Неверный формат. Используйте ГГГГ-ММ-ДД.")

def main():
    print("ПАРСЕР ЦЕН НА ДРАГ. МЕТАЛЛЫ ЦБ РФ")
    print("=" * 50)

    if not os.path.exists("parsed_data/metal_prices.json"):
        print("Данных нет — запускаем сбор…")
        if not ParserCBRF().start():
            print("Сбор не удался. Выход.")
            return
        print()

    try:
        db = MetalPricesCBRF()
    except Exception as e:
        print(e)
        return

    while True:
        print("\n1. Цены на дату")
        print("2. Последние цены")
        print("3. Цены за период")
        print("4. Выход")
        choice = input("→ ").strip()

        if choice == "1":
            d = _input_date("Дата (ГГГГ-ММ-ДД): ")
            pr = db.prices_by_date(d)
            if pr:
                print(f"\n{d}:")
                for m, p in pr.items():
                    print(f"  {db.display_name(m)}: {p} руб/г")
            else:
                print("Нет данных за эту дату")

        elif choice == "2":
            pr, d = db.prices_last()
            if pr:
                print(f"\nПоследние цены ({d}):")
                for m, p in pr.items():
                    print(f"  {db.display_name(m)}: {p} руб/г")

        elif choice == "3":
            d1 = _input_date("С (ГГГГ-ММ-ДД): ")
            d2 = _input_date("По (ГГГГ-ММ-ДД): ")
            rng = db.prices_range(d1, d2)
            if rng:
                print(f"\nЦены с {d1} по {d2}:")
                for d, pr in rng:
                    print(f"{d}:")
                    for m, p in pr.items():
                        print(f"  {db.display_name(m)}: {p} руб/г")
                print(f"Всего дней: {len(rng)}")
            else:
                print("В указанном диапазоне данных нет")

        elif choice == "4":
            print("Выход.")
            break
        else:
            print("Выберите 1-4")

if __name__ == "__main__":
    main()
