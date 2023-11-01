from datetime import timedelta
from database import collection as sample_collection
from service import *

group_time_format = {
    "month": "%Y-%m-01T00:00:00",
    "day": "%Y-%m-%dT00:00:00",
    "hour": "%Y-%m-%dT%H:00:00",
}


def get_next_date(current_date, group_type, dt_upto):
    next_date = current_date

    if group_type == "hour":
        next_date += timedelta(hours=1)
    elif group_type == "day":
        next_date += timedelta(days=1)
    elif group_type == "month":
        if current_date.month == 12:
            next_date = next_date.replace(year=current_date.year + 1, month=1)
        else:
            next_date = next_date.replace(month=current_date.month + 1)

    # Проверка, чтобы не превысить значение dt_upto
    return min(next_date, dt_upto)


async def add_data_to_dataset(dataset, labels, result_pipeline, date_format, current_date):
    if not result_pipeline:  # Если данных нет, добавляем 0
        dataset.append(0)
        labels.append(current_date.strftime(date_format))
    else:  # Иначе добавляем фактические значения
        for doc in result_pipeline:
            dataset.append(doc["totalValue"])
            labels.append(doc["_id"])


async def aggregator(dt_from, dt_upto, group_type):
    if group_type not in group_time_format:
        return None

    date_format = group_time_format[group_type]

    dataset = []
    labels = []

    current_date = dt_from

    while current_date <= dt_upto:
        next_date = get_next_date(current_date, group_type, dt_upto)

        if next_date == current_date and group_type != "hour":
            break  # Выход из цикла, если достигнут конец временного диапазона

        result_pipeline = [
            {
                "$match": {"dt": {"$gte": current_date, "$lt": next_date}}
            },
            {
                "$group": {
                    "_id": {
                        "$dateToString": {"format": date_format, "date": "$dt"}
                    },
                    "totalValue": {"$sum": "$value"},
                },
            },
            {"$sort": {"_id": 1}},
        ]

        aggregated_data = await sample_collection.aggregate(result_pipeline).to_list(None)
        await add_data_to_dataset(dataset, labels, aggregated_data, date_format, current_date)

        print(current_date)

        if current_date == dt_upto and group_type == "hour":
            break
        current_date = next_date

    return {"dataset": dataset, "labels": labels}
