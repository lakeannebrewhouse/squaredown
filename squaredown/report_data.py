"""Accounting sales data functions.
"""
# pyright: reportOptionalMemberAccess=false

from datetime import datetime

from aracnid_logger import Logger
from i_mongodb import MongoDBInterface


# initialize logging
logger = Logger(__name__).get_logger()


class ReportData():
    """ReportData class.
    """
    def __init__(self, mdb=None) -> None:
        """Initializes the ReportData class.

        Args:
            mdb: A reference to a MongoDBInterface object.
        """
        # initialize MongoDB interface
        self.mdb = mdb
        if not mdb:
            self.mdb = MongoDBInterface().get_mdb()

        # initialize data
        self.data = {}

    def get_data(self, start=None, end=None):
        """Retrieves the report data form MongoDB for the given timespan.
        """
        # initialize data
        self.data = self.init_data(start, end)

        # calculate sales
        self.set_sales_data()
        self.set_gift_card_sales_data()
        self.set_processing_fee()
        self.set_category_sales_data()
        self.set_collected_sales_data()
        self.set_service_charge_data()

        # calculate refunds
        #set_gift_card_refund_data()
        self.set_refund_data()
        self.set_processing_fee_refund()
        self.set_category_refund_data()
        self.set_collected_refund_data()

        # calculate cost
        self.set_cost_sales_data()

        self.calculate_net()

        return self.data

    def init_data(self, start, end):
        """Initialize the report data.

        Args:
            start: Start of the timespan.
            end: End of the timespan.

        Returns:
            A structured data dictionary for the accounting report.
        """
        self.data = {
            'timespan': {
                'start': start.isoformat(),
                'end': end.isoformat()
            },
            'summary': {
                'gross': {'sales': 0, 'refunds': 0, 'net': 0},
                'discount': {'sales': 0, 'refunds': 0, 'net': 0},
                'net': {'sales': 0, 'refunds': 0, 'net': 0},
                'tax': {'sales': 0, 'refunds': 0, 'net': 0},
                'tip': {'sales': 0, 'refunds': 0, 'net': 0},
                'gift_card': {'sales': 0, 'refunds': 0, 'net': 0},
                'partial_refund': {'sales': 0, 'refunds': 0, 'net': 0},
                'fee': {'sales': 0, 'refunds': 0, 'net': 0},
                'gift_card_load': {'sales': 0, 'refunds': 0, 'net': 0},
                'net_total': {'sales': 0, 'refunds': 0, 'net': 0}
            },
            'collected': {
                'total': {'sales': 0, 'refunds': 0, 'net': 0},
                'cash': {'sales': 0, 'refunds': 0, 'net': 0},
                'card': {'sales': 0, 'refunds': 0, 'net': 0},
                'square_gift_card': {'sales': 0, 'refunds': 0, 'net': 0},
                'buy_now_pay_later': {'sales': 0, 'refunds': 0, 'net': 0},
                'other': {'sales': 0, 'refunds': 0, 'net': 0},
                'wallet': {'sales': 0, 'refunds': 0, 'net': 0}
            },
            'category_sales': {
                'total': {'sales': 0, 'refunds': 0, 'net': 0},
                'uncategorized': {'sales': 0, 'refunds': 0, 'net': 0},
            },
            'cost': {
                'total': {'sales': 0, 'refunds': 0, 'net': 0},
                'uncategorized': {'sales': 0, 'refunds': 0, 'net': 0},        }
        }

        return self.data

    def set_gift_card_sales_data(self):
        """Sets the gift card sales data from MongoDB for the given timespan.

        Note: There may be an issues with tips on gift card sales.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'state': 'COMPLETED'
                }
            }, {
                '$unwind': {
                    'path': '$line_items',
                    'includeArrayIndex': 'line_item_index',
                    'preserveNullAndEmptyArrays': False
                }
            }, {
                '$match': {
                    'line_items.name': {'$in': ['Gift Card', 'eGift Card']}
                }
            }, {
                '$addFields': {
                    'gross_sales_money': '$line_items.gross_sales_money',
                    'discount_money': '$line_items.total_discount_money',
                    'tax_money': '$line_items.total_tax_money'
                }
            }, {
                '$unset': [
                    'fulfillments',
                    'net_amounts',
                    'total_discount_money',
                    'total_money',
                    'total_tax_money'
                ]
            }, {
                '$set': {
                    'total_tip_money.amount': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    '$line_item_index', 0
                                ]
                            },
                            'then': 0,
                            'else': '$total_tip_money.amount'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'total_gross_sales_money_amount': {
                        '$sum': '$gross_sales_money.amount'
                    },
                    'total_discount_money_amount': {
                        '$sum': '$discount_money.amount'
                    },
                    'total_tax_money_amount': {
                        '$sum': '$tax_money.amount'
                    },
                    'total_tip_money_amount': {
                        '$sum': '$total_tip_money.amount'
                    }
                }
            }, {
                '$addFields': {
                    'total_net_sales_money_amount': {
                        '$subtract': [
                            '$total_gross_sales_money_amount',
                            '$total_discount_money_amount'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'total_collected_money_amount': {
                        '$add': [
                            '$total_net_sales_money_amount',
                            '$total_tax_money_amount',
                            '$total_tip_money_amount'
                        ]
                    }
                }
            }
        ]

        results = list(self.mdb.square_orders.aggregate(pipeline=pipeline))
        if results:
            self.data['summary']['gift_card']['sales'] = (
                results[0]['total_gross_sales_money_amount']
            )

            # add any tip amounts
            self.data['summary']['tip']['sales'] += (
                results[0]['total_tip_money_amount']
            )

            # add any gift card discounts
            self.data['summary']['discount']['sales'] -= (
                results[0]['total_discount_money_amount']
            )
            self.data['summary']['net']['sales'] -= (
                results[0]['total_discount_money_amount']
            )

    def set_sales_data(self):
        """Get the sales data from MongoDB for the given timespan.

        Note: Gift card itemizations are filtered out.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'state': {'$in': ['COMPLETED', 'OPEN']}
                }
            }, {
                '$unwind': {
                    'path': '$line_items',
                    'includeArrayIndex': 'line_item_index',
                    'preserveNullAndEmptyArrays': False
                }
            }, {
                '$match': {
                    'line_items.name': {'$nin': ['Gift Card', 'eGift Card']}
                }
            }, {
                '$addFields': {
                    'gross_sales_money': '$line_items.gross_sales_money',
                    'discount_money': '$line_items.total_discount_money',
                    'tax_money': '$line_items.total_tax_money'
                }
            }, {
                '$unset': [
                    'fulfillments',
                    'net_amounts',
                    'total_discount_money',
                    'total_money',
                    'total_tax_money'
                ]
            }, {
                '$set': {
                    'total_tip_money.amount': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    '$line_item_index', 0
                                ]
                            },
                            'then': 0,
                            'else': '$total_tip_money.amount'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'total_gross_sales_money_amount': {
                        '$sum': '$gross_sales_money.amount'
                    },
                    'total_discount_money_amount': {
                        '$sum': '$discount_money.amount'
                    },
                    'total_tax_money_amount': {
                        '$sum': '$tax_money.amount'
                    },
                    'total_tip_money_amount': {
                        '$sum': '$total_tip_money.amount'
                    }
                }
            }, {
                '$addFields': {
                    'total_net_sales_money_amount': {
                        '$subtract': [
                            '$total_gross_sales_money_amount',
                            '$total_discount_money_amount'
                        ]
                    }
                }
            }, {
                '$addFields': {
                    'total_collected_money_amount': {
                        '$add': [
                            '$total_net_sales_money_amount',
                            '$total_tax_money_amount',
                            '$total_tip_money_amount'
                        ]
                    }
                }
            }
        ]

        results = list(self.mdb.square_orders.aggregate(pipeline=pipeline))
        if results:
            sales_data = results[0]

            self.data['summary']['gross']['sales'] = (
                sales_data['total_gross_sales_money_amount']
            )
            self.data['summary']['discount']['sales'] = (
                -sales_data['total_discount_money_amount']
            )
            self.data['summary']['net']['sales'] = (
                sales_data['total_net_sales_money_amount']
            )
            self.data['summary']['tax']['sales'] = sales_data['total_tax_money_amount']
            self.data['summary']['tip']['sales'] = sales_data['total_tip_money_amount']

    def set_processing_fee(self):
        """Get the total processing fees from MongoDB for the given timespan.
        """
        # get the credit card processing fees
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'status': 'COMPLETED'
                }
            }, {
                '$unwind': {
                    'path': '$processing_fee'
                }
            }, {
                '$group': {
                    '_id': None,
                    'amount': {
                        '$sum': '$processing_fee.amount_money.amount'
                    }
                }
            }
        ]
        results = list(self.mdb.square_payments.aggregate(pipeline=pipeline))
        if results:
            # set the fees in the data structure
            self.data['summary']['fee']['sales'] = -results[0]['amount']

        # get gift card load fees
        pipeline = [
            {
                '$match': {
                    'effective_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    }, 
                    'type': 'OTHER'
                }
            }, {
                '$group': {
                    '_id': None, 
                    'amount': {
                        '$sum': '$gross_amount_money.amount'
                    }
                }
            }
        ]
        results = list(self.mdb.square_payout_entries.aggregate(pipeline=pipeline))
        if results:
            # set the fees in the data structure
            self.data['summary']['gift_card_load']['sales'] = results[0]['amount']

    def set_category_sales_data(self):
        """Get the category sales data from MongoDB for the given timespan.
        """
        pipeline = [
            {
                '$match': {
                    'order_created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'order_state': {'$in': ['COMPLETED', 'OPEN']},
                    'category_name': {'$exists': 1}
                }
            }, {
                '$addFields': {
                    'category_name': {
                        '$toLower': '$category_name'
                    }
                }
            }, {
                '$group': {
                    '_id': '$category_name',
                    'gross_sales_money_amount': {
                        '$sum': '$gross_sales_money.amount'
                    }
                }
            }
        ]

        category_data = list(self.mdb.square_order_itemizations.aggregate(
            pipeline=pipeline
        ))

        total_category_sales = 0
        for category in category_data:
            # get the category name
            category_name = category['_id']
            if not category_name:
                logger.error('Unknown category')
                category_name = 'uncategorized'

            # get the sales amount
            category_amount = category['gross_sales_money_amount']
            total_category_sales += category_amount

            # initialize category, if necessary
            if category_name not in self.data['category_sales']:
                self.data['category_sales'][category_name] = {
                    'sales': 0, 'refunds': 0, 'net': 0
                }

            # set sales amount for the category
            self.data['category_sales'][category_name]['sales'] = category_amount
        
        # set the total sales amount
        self.data['category_sales']['total']['sales'] = total_category_sales

    def set_collected_sales_data(self):
        """Get the tender data from MongoDB for the given timespan.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'order_state': {'$in': ['COMPLETED', 'OPEN']}
                }
            }, {
                '$group': {
                    '_id': {
                        '$toLower': '$type'
                    },
                    'amount': {
                        '$sum': '$amount_money.amount'
                    }
                }
            }, {
                '$match': {
                    '_id': {
                        '$ne': 'no_sale'
                    }
                }
            }
        ]

        tender_data = list(self.mdb.square_order_tenders.aggregate(pipeline=pipeline))
        total_collected_sales = 0
        for tender_type in tender_data:
            tender_name = tender_type['_id']
            tender_amount = tender_type['amount']
            total_collected_sales += tender_amount
            self.data['collected'][tender_name]['sales'] = tender_amount
        self.data['collected']['total']['sales'] = total_collected_sales

    def set_service_charge_data(self):
        """Get the service charges from MongoDB for the given timespan.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'state': {
                        '$in': [
                            'COMPLETED'
                        ]
                    }
                }
            }, {
                '$unwind': {
                    'path': '$service_charges', 
                    'preserveNullAndEmptyArrays': False
                }
            }, {
                '$group': {
                    '_id': '$service_charges.name', 
                    'amount': {
                        '$sum': '$service_charges.total_money.amount'
                    }
                }
            }
        ]

        results = list(self.mdb.square_orders.aggregate(pipeline=pipeline))
        if results:
            for service_charge in results:
                service_charge_name = service_charge['_id']
                if service_charge_name == 'Gratuity':
                    self.data['summary']['tip']['sales'] += service_charge['amount']
                else:
                    logger.error('unhandled service charge found: %s', service_charge_name)

    def set_refund_data(self):
        """Get the refund data from MongoDB for the given timespan.
        """
        self.set_refund_data_returns()
        self.set_refund_data_returns_custom()
        self.set_refund_data_tips()

    def set_refund_data_returns(self):
        """Get the refund data from MongoDB for the given timespan.

        This function processes refunds from returns.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'state': {'$in': ['COMPLETED']},
                    'refunds': {
                        '$exists': 1
                    }
                }
            }, {
                '$unwind': {
                    'path': '$returns'
                }
            }, {
                '$unwind': {
                    'path': '$returns.return_line_items',
                    'includeArrayIndex': 'line_item_index'
                }
            }, {
                '$unwind': {
                    'path': '$returns.return_line_items.return_modifiers',
                    'preserveNullAndEmptyArrays': True
                }
            }, {
                '$unwind': {
                    'path': '$returns.return_taxes'
                }
            }, {
                '$addFields': {
                    'base_refund_money': '$returns.return_line_items.gross_return_money',
                    'modifier_refund_money': {
                        '$ifNull': [
                            '$returns.return_line_items.return_modifiers.total_price_money', {
                                'amount': 0,
                                'currency': 'USD'
                            }
                        ]
                    },
                    'discount_money': '$returns.return_line_items.total_discount_money',
                    'tax_money': '$returns.return_line_items.total_tax_money'
                }
            }, {
                '$addFields': {
                    'gross_refund_money': {
                        'amount': {
                            '$add': [
                                '$base_refund_money.amount', '$modifier_refund_money.amount'
                            ]
                        },
                        'currency': 'USD'
                    }
                }
            }, {
                '$addFields': {
                    'gross_refund_money_amount': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$returns.return_taxes.type', 'INCLUSIVE'
                                ]
                            }, 
                            'then': {
                                '$subtract': [
                                    '$gross_refund_money.amount', '$tax_money.amount'
                                ]
                            }, 
                            'else': '$gross_refund_money.amount'
                        }
                    }
                }
            }, {
                '$addFields': {
                    'net_refund_money_amount': {
                        '$subtract': [
                            '$gross_refund_money_amount', '$discount_money.amount'
                        ]
                    }
                }
            }, {
                '$unset': [
                    'fulfillments',
                    'net_amounts',
                    'total_discount_money',
                    'total_money',
                    'total_tax_money',
                    'total_tip_money'
                ]
            }, {
                '$set': {
                    'tip_money.amount': {
                        '$cond': {
                            'if': {
                                '$gt': [
                                    '$line_item_index', 0
                                ]
                            },
                            'then': 0,
                            'else': '$return_amounts.tip_money.amount'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': None,
                    'total_gross_refund_money_amount': {
                        '$sum': '$gross_refund_money_amount'
                    },
                    'total_net_refund_money_amount': {
                        '$sum': '$net_refund_money_amount'
                    },
                    'total_discount_refund_money_amount': {
                        '$sum': '$discount_money.amount'
                    },
                    'total_tax_refund_money_amount': {
                        '$sum': '$tax_money.amount'
                    },
                    'total_tip_refund_money_amount': {
                        '$sum': '$tip_money.amount'
                    }
                }
            }, {
                '$addFields': {
                    'total_collected_refund_money_amount': {
                        '$add': [
                            '$total_net_refund_money_amount',
                            '$total_tax_refund_money_amount',
                            '$total_tip_refund_money_amount'
                        ]
                    }
                }
            }
        ]

        results = list(self.mdb.square_orders.aggregate(pipeline=pipeline))

        if results:
            refund_data = results[0]
            if refund_data:
                self.data['summary']['gross']['refunds'] = (
                    -refund_data['total_gross_refund_money_amount']
                )
                self.data['summary']['discount']['refunds'] = (
                    refund_data['total_discount_refund_money_amount']
                )
                self.data['summary']['net']['refunds'] = (
                    -refund_data['total_net_refund_money_amount']
                )
                self.data['summary']['tax']['refunds'] = (
                    -refund_data['total_tax_refund_money_amount']
                )
                self.data['summary']['tip']['refunds'] = (
                    -refund_data['total_tip_refund_money_amount']
                )

    def set_refund_data_returns_custom(self):
        """Get the custom refund data from MongoDB for the given timespan.

        This function processes custom refunds from returns.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'state': {
                        '$in': [
                            'COMPLETED'
                        ]
                    },
                    'refunds': {
                        '$exists': 1
                    }
                }
            }, {
                '$unwind': {
                    'path': '$returns'
                }
            }, {
                '$unwind': {
                    'path': '$returns.return_line_items', 
                    'includeArrayIndex': 'line_item_index'
                }
            }, {
                '$match': {
                    'returns.return_line_items.item_type': 'CUSTOM_AMOUNT'
                }
            }, {
                '$group': {
                    '_id': None, 
                    'total_gross_refund_money_amount': {
                        '$sum': '$returns.return_line_items.gross_return_money.amount'
                    }
                }
            }
        ]

        results = list(self.mdb.square_orders.aggregate(pipeline=pipeline))

        if results:
            refund_data = results[0]
            if refund_data:
                self.data['summary']['partial_refund']['refunds'] = (
                    -refund_data['total_gross_refund_money_amount']
                )

    def set_refund_data_tips(self):
        """Get the refund data from MongoDB for the given timespan.

        This function finds the special case where a tip is refunded without
        returning any items.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'state': {'$in': ['COMPLETED']},
                    'refunds': {'$exists': 1},
                    'returns.return_line_items': {'$exists': 0}
                }
            }, {
                '$group': {
                    '_id': None,
                    'total_discount_refund_money_amount': {
                        '$sum': '$return_amounts.discount_money.amount'
                    },
                    'total_tax_refund_money_amount': {
                        '$sum': '$return_amounts.tax_money.amount'
                    },
                    'total_tip_refund_money_amount': {
                        '$sum': '$return_amounts.tip_money.amount'
                    }
                }
            }
        ]

        results = list(self.mdb.square_orders.aggregate(pipeline=pipeline))

        if results:
            refund_data = results[0]
            if refund_data:
                self.data['summary']['discount']['refunds'] += (
                    refund_data['total_discount_refund_money_amount']
                )
                self.data['summary']['net']['refunds'] += (
                    refund_data['total_discount_refund_money_amount']
                )
                self.data['summary']['tax']['refunds'] -= (
                    refund_data['total_tax_refund_money_amount']
                )
                self.data['summary']['tip']['refunds'] -= (
                    refund_data['total_tip_refund_money_amount']
                )

    def set_processing_fee_refund(self):
        """Get the total processing fees from MongoDB for the given timespan.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'status': 'COMPLETED'
                }
            }, {
                '$unwind': {
                    'path': '$processing_fee'
                }
            }, {
                '$group': {
                    '_id': None,
                    'amount': {
                        '$sum': '$processing_fee.amount_money.amount'
                    }
                }
            }
        ]

        results = list(self.mdb.square_refunds.aggregate(pipeline=pipeline))

        if results:
            self.data['summary']['fee']['refunds'] = -results[0]['amount']

    def set_category_refund_data(self):
        """Get the category return data from MongoDB for the given timespan.
        """
        pipeline = [
            {
                '$match': {
                    'order_created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    }, 
                    'order_state': {
                        '$in': [
                            'COMPLETED', 'OPEN'
                        ]
                    }, 
                    'category_name': {
                        '$exists': 1
                    }, 
                    'itemization_type': 'return'
                }
            }, {
                '$addFields': {
                    'category_name': {
                        '$toLower': '$category_name'
                    }
                }
            }, {
                '$lookup': {
                    'from': 'square_orders', 
                    'localField': 'order_id', 
                    'foreignField': '_id', 
                    'as': 'order'
                }
            }, {
                '$unwind': {
                    'path': '$order'
                }
            }, {
                '$unwind': {
                    'path': '$order.returns'
                }
            }, {
                '$unwind': {
                    'path': '$order.returns.return_taxes'
                }
            }, {
                '$addFields': {
                    'gross_return_money_amount': {
                        '$cond': {
                            'if': {
                                '$eq': [
                                    '$order.returns.return_taxes.type', 'INCLUSIVE'
                                ]
                            }, 
                            'then': {
                                '$subtract': [
                                    '$gross_return_money.amount', '$total_tax_money.amount'
                                ]
                            }, 
                            'else': '$gross_return_money.amount'
                        }
                    }
                }
            }, {
                '$group': {
                    '_id': '$category_name', 
                    'gross_return_money_amount': {
                        '$sum': '$gross_return_money_amount'
                    }
                }
            }
        ]

        category_return_data = list(self.mdb.square_order_itemizations.aggregate(
            pipeline=pipeline
        ))
        total_category_returns = 0
        for category in category_return_data:
            # get the category name
            category_name = category['_id']
            if not category_name:
                logger.error('Unknown category')
                category_name = 'uncategorized'

            # get the refund amount
            category_amount = category['gross_return_money_amount']
            total_category_returns += category_amount

            # initialize category, if necessary
            if category_name not in self.data['category_sales']:
                self.data['category_sales'][category_name] = {
                    'sales': 0, 'refunds': 0, 'net': 0
                }

            # set refund amount for the category
            self.data['category_sales'][category_name]['refunds'] = -category_amount

        # set the total refund amount
        self.data['category_sales']['total']['refunds'] = -total_category_returns

    def set_collected_refund_data(self):
        """Get the refund tender data from MongoDB for the given timespan.
        """
        pipeline = [
            {
                '$match': {
                    'created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'state': 'COMPLETED',
                    'return_amounts.total_money.amount': {
                        '$gt': 0
                    }
                }
            }, {
                '$unwind': {
                    'path': '$refunds',
                    'includeArrayIndex': 'refund_index',
                    'preserveNullAndEmptyArrays': False
                }
            }, {
                '$lookup': {
                    'from': 'square_order_tenders',
                    'localField': 'refunds.tender_id',
                    'foreignField': '_id',
                    'as': 'square_order_tenders'
                }
            }, {
                '$unwind': {
                    'path': '$square_order_tenders'
                }
            }, {
                '$addFields': {
                    'type': {
                        '$toLower': '$square_order_tenders.type'
                    }
                }
            }, {
                '$group': {
                    '_id': '$type',
                    'amount': {
                        '$sum': '$return_amounts.total_money.amount'
                    }
                }
            }
        ]

        tender_data = list(self.mdb.square_orders.aggregate(pipeline=pipeline))

        total_collected_sales = 0
        for tender_type in tender_data:
            tender_name = tender_type['_id']
            tender_amount = tender_type['amount']
            total_collected_sales += tender_amount
            self.data['collected'][tender_name]['refunds'] = -tender_amount
        self.data['collected']['total']['refunds'] = -total_collected_sales

    def calculate_net(self):
        """Calculates the net value from the sales and the refunds.
        """
        # calculate net total
        self.data['summary']['net_total']['sales'] = (
            self.data['collected']['total']['sales'] + self.data['summary']['fee']['sales']
        )

        # calculate net total refund
        self.data['summary']['net_total']['refunds'] = (
            self.data['collected']['total']['refunds'] +
            self.data['summary']['fee']['refunds']
        )

        # calculate all net
        headings = ['summary', 'collected', 'category_sales', 'cost']
        for heading in headings:
            subheadings = self.data[heading]
            for sub in subheadings:
                self.data[heading][sub]['net'] = (
                    self.data[heading][sub]['sales'] + self.data[heading][sub]['refunds']
                )

    def set_cost_sales_data(self):
        """Get the cost sales data from MongoDB for the given timespan.
        """
        pipeline = [
            {
                '$match': {
                    'order_created_at': {
                        '$gte': datetime.fromisoformat(self.data['timespan']['start']),
                        '$lt': datetime.fromisoformat(self.data['timespan']['end'])
                    },
                    'order_state': 'COMPLETED'
                }
            }, {
                '$unwind': {
                    'path': '$applied_costs'
                }
            }, {
                '$group': {
                    '_id': '$applied_costs.category',
                    'amount': {
                        '$sum': '$applied_costs.applied_money.amount'
                    }
                }
            }
        ]

        total_cost_sales = 0
        results = list(self.mdb.square_order_itemizations.aggregate(pipeline=pipeline))
        for category in results:
            # get the category name
            category_name = category['_id']
            if category_name == 'gift card':
                continue
            if not category_name:
                logger.error('Unknown category')
                category_name = 'uncategorized'

            # get the cost amount
            category_amount = category['amount']
            total_cost_sales += category_amount

            # initialize category, if necessary
            if category_name not in self.data['cost']:
                self.data['cost'][category_name] = {
                    'sales': 0, 'refunds': 0, 'net': 0
                }

            # set cost amount for the category
            self.data['cost'][category_name]['sales'] = category_amount

        # set the total cost
        self.data['cost']['total']['sales'] = total_cost_sales
