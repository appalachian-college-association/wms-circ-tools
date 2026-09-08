# OpenRefine specs

Reference files for the manual OpenRefine steps these tools grew out of. They are kept in
version control so the Python scripts that replace them can be checked against the original
logic. None of them is read by any script.

| File | What it is | How to use it in OpenRefine |
|------|------------|-----------------------------|
| `WVBpatron_check_source.json` | GREL operation history that classifies WVB patrons by whether their record has a campus (`@bethanywv.edu` + `sts.windows.net`) source pair, then fills `patron_barcode_old/new`, `idAtSource`, `sourceSystem` for the ones that don't. **Superseded by `check_source.py`** (which drops the `cell.cross` delete-list lookup in step 3). | Import a report from `reports/ABC/patrons_openrefine/`, open **Undo/Redo → Apply**, paste the file contents. |
| `open_refine_option_code_patron_updates.json` | Export option code producing a tab-delimited `patron_updates.txt` with the four columns `circ_patron_reload.py` expects. | **Export → Custom tabular…**, open the **Option Code** tab, paste, then save the result as `patron_updates.txt` in the project root. |
| `open_refine_option_code_reload_format.json` | Export option code that restores the original pipe-delimited, fully quoted layout of a `*_Patron_Report_Full*` report after editing it in OpenRefine, so it can be fed to `circ_patron_reload.py --input-file`. | **Export → Custom tabular…**, **Option Code** tab, paste. |

Notes

- OpenRefine chokes on `#` in these reports; use `data_fetcher_openrefine.py` to download copies with
  `#` replaced by `[hashmark]` into `reports/ABC/*_openrefine/`.
- Operation histories embed column names and, for `cell.cross`, the name of another OpenRefine project
  (`WVBpatronsdelete110325`). They only replay cleanly against a project with the same columns.
