from datetime import datetime, timezone

from app.manager.Marquez import Marquez

if __name__ == "__main__":

    Marquez.event( 
        event_type="COMPLETE",
        run_id="123e4567-e89b-12d3-a456-426614174000",
        event_time=datetime.now(timezone.utc).isoformat(),
        job_namespace="hutchai_lol",
        job_name="ZZZ_lol_enrich_fact_matchs",
        description="Job d'enrichissement des faits de matchs LOL",
        inputs=[
            Marquez.build_dataset(
                namespace="hutchai_lol",
                name="lol_fact_puuid_to_process",
                fields=[
                    {"name": "puuid", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"},
                ]
            )
        ],
        outputs=[
            Marquez.build_dataset(
                namespace="hutchai_lol",
                name="ZZZ_lol_fact_datas.lol_fact_match",
                fields=[
                    {"name": "puuid", "type": "VARCHAR"},
                    {"name": "status", "type": "VARCHAR"},
                ]
            )
        ],
    )