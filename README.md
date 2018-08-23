# GOB-Core
GOB Core components

Include in a GOB project using:

```bash
pip install -e git+git://github.com/Amsterdam/GOB-Core.git#egg=gobcore

```

To link to the latest release remove the commit id as follows:

```bash
pip freeze --local > src/requiremenets.txt
```

Example:
In requirements.txt:
```
-e git://github.com/Amsterdam/GOB-Core.git@909301b10ec20f839d1db13fe2bf512ab56c3960#egg=gobcore
```
becomes
```
-e git://github.com/Amsterdam/GOB-Core.git#egg=gobcore
```
