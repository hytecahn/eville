"""Passive labels from operator-confirmed observations; never trigger device actions."""
LABELS = {
    '33:01:43:01': ('엘리베이터 호출·도착 관련 신호', 'elevator_call_arrival'),
    '33:01:C3:03': ('엘리베이터 호출 관련 응답', 'elevator_call_response'),
}


def classify(item):
    item = dict(item)
    if item.get('signature') in LABELS:
        label, category = LABELS[item['signature']]
        item.update(known=True, label=label, category=category,
                    classification_basis='operator_confirmed_elevator_call',
                    arrival_verified=False)
    return item
