import argparse
import pickle
import time
from datetime import datetime
from typing import List

import tinvest
from constants import (
    TOKEN,
    MAX_PRICE_USD,
    USD_TO_RUB,
    PARSED_BONDS_FILE,
    MIN_RATIO,
    DEBUG
)
from tinvest.schemas import (
    LimitOrderRequest,
    MarketInstrument,
    OperationType,
    Currency,
    OperationStatus, OrderResponse, Orderbook)

client = tinvest.SyncClient(TOKEN)
portfolio = tinvest.PortfolioApi(client)
market = tinvest.MarketApi(client)
orders = tinvest.OrdersApi(client)


def round_float(x: float):
    return round(x, 3)


def print_log(param: str):
    if DEBUG:
        print(param)


class CompanyData:

    def __init__(self, figi: str, ticker: str, ask: OrderResponse, bid: OrderResponse, currency: Currency,
                 last_price: float,
                 min_price_increment: float, close_price: float = 0):
        self.figi = figi
        self.ticker = ticker
        self.ask = ask
        self.bid = bid
        self.currency = currency
        self.last_price = last_price
        self.minPriceIncrement = min_price_increment
        self.close_price = close_price

    def is_changed(self):
        if self.last_price is None or self.close_price is None:
            return False
        return round_float(self.last_price) != round_float(self.close_price)

    def get_income(self):
        return round_float(self.get_delta() / self.last_price)

    def get_delta(self):
        return self.close_price - self.get_bid()

    def __str__(self):
        return 'figi: {}, delta: {}, last_price: {}, income: {}, bid_price: {}, ticker: {}, currency: {}, is_changed:{}'. \
            format(self.figi, round_float(self.get_delta()), round_float(self.last_price),
                   self.get_income(), round_float(self.bid.price),
                   self.ticker, self.currency, self.is_changed())

    def __lt__(self, other):
        if (self.get_delta() / self.last_price) < (other.get_delta() / other.last_price):
            return True
        else:
            return False

    def get_bid(self):
        return self.bid.price

    def get_ask(self):
        return self.ask.price


def get_price(figi: str) -> Orderbook:
    info = market.market_orderbook_get(figi, 1)
    if info.status_code == 200:
        stock_info = info.parse_json().payload
        if len(stock_info.asks) == 0:
            stock_info.asks.append(OrderResponse(quantity=0, price=0))
        if len(stock_info.bids) == 0:
            stock_info.bids.append(OrderResponse(quantity=0, price=10000))
        return stock_info
    else:
        raise Exception(info.status_code)


def float_eq(x: float, y: float):
    return round(x, 3) == round(y, 3)


def get_info_by_figi(figi: str):
    response = market.market_search_by_figi_get(figi)
    if response.status_code == 200:
        cmp_info = response.parse_json().payload
        prices = get_price(figi)
        return CompanyData(figi, cmp_info.ticker, prices.asks[0], prices.bids[0], cmp_info.currency,
                           prices.last_price, cmp_info.min_price_increment, prices.close_price)
    return None


def create_company(bond: MarketInstrument):
    """Create object CompanyData from variable bond"""
    try:
        stock_info = get_price(bond.figi)
        return CompanyData(bond.figi, bond.ticker, stock_info.asks[0], stock_info.bids[0], bond.currency,
                           stock_info.last_price, stock_info.min_price_increment, stock_info.close_price)

    except Exception as e:
        print("Exception", e)
        if str(e) == "429":
            time.sleep(60)
            return create_company(bond)
        return None


def parse_stocks(number_to_parse=-1):
    response = market.market_stocks_get()
    if response.status_code != 200:
        print(response.parse_error())
        return None
    all_stocks = response.parse_json().payload.instruments
    if number_to_parse == -1:
        number_to_parse = len(all_stocks)
    res_companies = list()
    iter = 0
    for bond in all_stocks:
        if len(res_companies) >= number_to_parse:
            break
        iter += 1
        # if iter % 110 == 0:
        #     time.sleep(60)
        tmp = create_company(bond)
        if tmp is not None:
            res_companies.append(tmp)
        print(len(res_companies))
    print('\7\7\7\7')
    return res_companies


def print_to_file(output: str, companies: List[CompanyData]):
    f = open(output, "w+")
    companies.sort(reverse=True)
    for c in companies:
        print(c, file=f)


def is_valid_company(company: CompanyData, changed: bool, min_income: float, max_price: float):
    if company.currency == Currency.rub:
        value = company.last_price / USD_TO_RUB
    elif company.currency == Currency.usd:
        value = company.last_price
    else:
        print("Unknown currency {}".format(company.currency))
        return False
    if value > max_price or company.get_income() < min_income or (changed and not company.is_changed()):
        return False
    return True


def create_order(company: CompanyData, price: float, operation_type: OperationType, lots=1):
    body = LimitOrderRequest(price=round(price, 2), lots=lots, operation=operation_type)
    print_log("Create New Order: ticker {} price {} lots {} operation {} income: {}\n changes: {}Y/n"
              .format(company.ticker, body.price, body.lots, body.operation, company.get_income(),
                      company.is_changed()))
    response = orders.orders_limit_order_post(company.figi, body)
    if response.status_code == 200:
        print_log("order created")
        return True
    else:
        print_log(response.status_code)
        return False


def create_limit_order(company: CompanyData, operation_type: OperationType, lots=1):
    if operation_type == operation_type.buy:
        new_price = company.get_bid() + company.minPriceIncrement
    else:
        new_price = company.get_ask() - company.minPriceIncrement
    print("Are You sure: ticker {} price {} lots {} operation {} income: {}\n changes: {}Y/n"
          .format(company.ticker, round_float(new_price), lots, operation_type, company.get_income(),
                  company.is_changed()))
    return create_order(company, new_price, operation_type, lots)


def buy_companies(companies_list: List[CompanyData], changed: bool, min_ratio: float, max_price: float):
    for company in companies_list:
        if is_valid_company(company, changed, min_ratio, max_price) is True:
            print("buy ", company, "y/n or exit?")
            res = input()
            if res == "Y" or res == "y":
                response = create_limit_order(company, OperationType.buy)
                if response is False:
                    break
            elif res == "exit":
                print("exit")
                break


def update_active_orders(min_ratio: float):
    print_log("in update")
    orders_response = orders.orders_get()
    if orders_response.status_code == 200:
        orders_list = list(orders_response.parse_json().payload)
        for order in orders_list:
            company = get_info_by_figi(order.figi)
            if (order.operation == OperationType.buy and not float_eq(company.get_bid(), order.price)) or \
                    (order.operation == OperationType.sell and not float_eq(company.get_ask(), order.price)):
                print_log("You have to update {} order income: {}".format(order.operation, company.get_income()))
                response = orders.orders_cancel_post(order.order_id)
                print_log(str(company.get_income()))
                if (company.get_income() > min_ratio and order.operation == OperationType.buy) or \
                    (abs(company.get_income()) > min_ratio and order.operation == OperationType.sell):
                    create_limit_order(company, order.operation)
    else:
        raise Exception("can't update orders")


def parse_arguments():
    global DEBUG
    parser = argparse.ArgumentParser()
    parser.add_argument("-g", help="Turn on Debug", action="store_true", default=False)
    parser.add_argument("--max_price", help="set max price of stock in USD", action="store", default=MAX_PRICE_USD,
                        type=float)
    parser.add_argument("-o", help="set output directory to file with bonds",
                        action="store_true", default=PARSED_BONDS_FILE)
    parser.add_argument("--min_ratio", help="set min ratio to delete order", action="store", default=MIN_RATIO,
                        type=float)
    parser.add_argument("-c", "--changed", help="choose from order, that have at least 1 bargain today",
                        action="store_true", default=False)
    parser.add_argument("-f", help="load bonds info from file", action="store")
    parser.add_argument("--parse", help="update bonds info", action="store_true", default=False)
    parser.add_argument("--update", help="run infinite loop to update prices", action="store_true", default=False)
    args = parser.parse_args()
    DEBUG = args.g
    return args


def save_stocks(stocks, filename: str = "log_stocks.txt"):
    fp = open(filename, "wb")
    pickle.dump(stocks, fp)


def load_stocks(filename: str):
    fp = open(filename, "rb")
    return pickle.load(fp)


def check_done_orders(from_time: float):
    time_fmt = "%Y-%m-%dT%H:%M:%S.%f+03:00"
    from_time = datetime.fromtimestamp(from_time).strftime(time_fmt)
    to_time = datetime.now().strftime(time_fmt)
    print_log(from_time + " " + to_time)
    operations = tinvest.OperationsApi(client)
    response = operations.operations_get(from_time, to_time)
    if response.status_code == 200:
        operation_list = response.parse_json().payload.operations
        print("done operation length", len(operation_list))
        for operation in operation_list:
            if operation.status == OperationStatus.done:
                print('\7')  # make sound
                cmp_info = get_info_by_figi(operation.figi)
                if operation.operation_type == OperationType.buy:
                    operation_type = OperationType.sell
                else:
                    operation_type = OperationType.buy
                print("create order from prev done order")
                create_limit_order(cmp_info, operation_type)
            else:
                print("operation not done")
    else:
        raise Exception("Can't get operations")


if __name__ == "__main__":
    check_done_orders(time.time())
    args = parse_arguments()
    if args.parse or args.f is not None:
        if args.parse:
            data = parse_stocks()
            if data is None:
                raise Exception("Can't parse stocks")
            print_to_file(args.o, data)
            save_stocks(data)
        else:
            data = load_stocks(args.f)
        buy_companies(data, args.changed, args.min_ratio, args.max_price)
    if args.update:
        last_update = time.time()
        while True:
            check_done_orders(last_update)
            update_active_orders(args.min_ratio)
            last_update = time.time()
            time.sleep(20)
