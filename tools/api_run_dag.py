import datetime
import http.client

def run(dag, date_injection):
    print("Running the script...")
    print(f"DAG: {dag}")
    print(f"Date injection: {date_injection}")

    conn = http.client.HTTPConnection("localhost:8081")

    date = datetime.datetime.strptime(date_injection, "%Y-%m-%d").isoformat() + "Z"

    payload = "{\n\t\"logical_date\": \""+ date +"\",\n\t\"conf\": {\n\t\t\"date_injection\": \""+ str(date_injection) +"\"\n\t}\n}"

    headers = {
        'cookie': "session=875a15ef-f8ba-46dc-98b0-65732dc62d93.xztzRPvdOu7h028cbPlXTVuqcEw",
        'Content-Type': "application/json",
        'User-Agent': "insomnia/9.3.3",
        'Authorization': ""
        }

    conn.request("POST", f"/api/v2/dags/{dag}/dagRuns", payload, headers)

    res = conn.getresponse()
    data = res.read()

    print("Response status:", res.status)

    if res.status != 200:
        raise Exception(f"Request failed with status {res.status}: {data.decode('utf-8')}")

def past_sundays_run(dag, selected_years):

    selected_years = [int(y.strip()) for y in selected_years.split(",") if y.strip().isdigit()]

    if not selected_years or not isinstance(selected_years, list):
        print("Vous devez fournir au moins une année valide.")
        return
    today = datetime.date.today()
    last_sunday = today - datetime.timedelta(days=today.weekday() + 1) if today.weekday() != 6 else today
    sundays = []
    while last_sunday.year >= min(selected_years):
        if last_sunday.year in selected_years:
            sundays.append(last_sunday)
        last_sunday -= datetime.timedelta(days=7)
        if last_sunday < datetime.date(min(selected_years), 1, 1):
            break
    sundays.reverse()
    print("Lancement du DAG pour les dimanches passés :")
    for d in sundays:
        run(dag, d.strftime("%Y-%m-%d"))

def main():
    while True:

        dag = input("Nom du DAG à lancer : ")
        years = input("Entrez les années souhaitées séparées par des virgules (ex: 2023,2024) : ")

        print("Lancement tout les dimanches passés")
        print("Pour le DAG :", dag)
        print("Pour les années :", years)
        choice = input("Validez-vous le lancement ? (y/n) : ")

        if choice.lower() == "y":
            past_sundays_run(dag, years)
        else:
            print("Au revoir !")
            break

if __name__ == "__main__":
    main()