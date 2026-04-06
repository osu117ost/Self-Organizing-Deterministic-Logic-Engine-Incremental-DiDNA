# Executive Summary (RU)

Пакет собран в формате официальной стопки для передачи в спецификации, due diligence и исследовательские приложения.
- gate status: `pass`
- run_ids: `20260405T230026Z_0b28f298, 20260405T230137Z_5d7213c1, 20260405T162225Z_22f14610, 20260405T230146Z_5dbac053, 20260406T000140Z_669d8f63`

# Executive Summary (EN)

This package is organized as an official stack suitable for specifications, due diligence, and research appendices.
- gate status: `pass`
- run_ids: `20260405T230026Z_0b28f298, 20260405T230137Z_5d7213c1, 20260405T162225Z_22f14610, 20260405T230146Z_5dbac053, 20260406T000140Z_669d8f63`

```json
{
  "generated_utc": "2026-04-06T00:23:22Z",
  "overall_status": "pass",
  "failing_gates": [],
  "gates": {
    "correctness": {
      "status": "pass",
      "latency_monotonic_violations": 0,
      "negative_resource_violations": 0,
      "audit_reference_rows": 10,
      "audit_gap_bound": 2.0,
      "audit_gap_violations": 0
    },
    "reproducibility": {
      "status": "pass",
      "cv_threshold": 0.25,
      "max_observed_cv": 0.2400310284340078,
      "groups_with_repetition": 48
    },
    "investor": {
      "status": "pass",
      "required_tiers": [
        "desktop",
        "edge",
        "server"
      ],
      "required_tracks": [
        "ann",
        "core",
        "online"
      ],
      "tiers_with_all_tracks": [
        "desktop",
        "edge",
        "server"
      ],
      "tier_track_coverage": {
        "server": [
          "ann",
          "core",
          "online"
        ],
        "edge": [
          "ann",
          "core",
          "online"
        ],
        "desktop": [
          "ann",
          "core",
          "online"
        ]
      }
    },
    "scale": {
      "status": "pass",
      "ok_scales": [
        "100M",
        "10M",
        "1M"
      ],
      "required_minimum": [
        "1M",
        "10M"
      ],
      "required_final_dossier": "100M",
      "final_dossier_ready": true
    }
  }
}
```
