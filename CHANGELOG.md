# Changelog

## [0.3.5](https://github.com/BlueBrain/luigi-tools/compare/0.3.4..0.3.5)

> 27 August 2025

### Build

- Drop support for Python 3.8 (pre-commit-ci[bot] - [#119](https://github.com/BlueBrain/luigi-tools/pull/119))
- Freeze docutils to be compatible with m2r2 (Adrien Berchet - [#113](https://github.com/BlueBrain/luigi-tools/pull/113))

### Changes to Test Assets

- Fix for Py312 (Adrien Berchet - [#123](https://github.com/BlueBrain/luigi-tools/pull/123))
- Fix compatibility with pytest>=8 (Adrien Berchet - [#110](https://github.com/BlueBrain/luigi-tools/pull/110))
- Changing the working directory is no more needed and breaks pytest-html>=4 (Adrien Berchet - [#97](https://github.com/BlueBrain/luigi-tools/pull/97))

### CI Improvements

- Fix CodeCov upload (Adrien Berchet - [#95](https://github.com/BlueBrain/luigi-tools/pull/95))

## [0.3.4](https://github.com/BlueBrain/luigi-tools/compare/0.3.3..0.3.4)

> 21 April 2023

### Chores And Housekeeping

- Bump copier template (Adrien Berchet - [#87](https://github.com/BlueBrain/luigi-tools/pull/87))

### Refactoring and Updates

- Filter warnings when registering missing files in register_templates() (Adrien Berchet - [#86](https://github.com/BlueBrain/luigi-tools/pull/86))

## [0.3.3](https://github.com/BlueBrain/luigi-tools/compare/0.3.2..0.3.3)

> 8 March 2023

### New Features

- Add function to convert luigi config file into a dict (Adrien Berchet - [#83](https://github.com/BlueBrain/luigi-tools/pull/83))

## [0.3.2](https://github.com/BlueBrain/luigi-tools/compare/0.3.1..0.3.2)

> 3 March 2023

### New Features

- Set default resolution of dependency graphs to 300DPI (Adrien Berchet - [#80](https://github.com/BlueBrain/luigi-tools/pull/80))

## [0.3.1](https://github.com/BlueBrain/luigi-tools/compare/0.3.0..0.3.1)

> 2 March 2023

### Fixes

- The @copy_params decorator fails with parameter schemas (Adrien Berchet - [#78](https://github.com/BlueBrain/luigi-tools/pull/78))

<!-- auto-changelog-above -->

## [0.3.0](https://github.com/BlueBrain/luigi-tools/compare/0.2.1..0.3.0)

> 28 February 2023

### Deprecated Features

- JSON schema validation was moved to `luigi==3.2.0` (Adrien Berchet - [#71](https://github.com/BlueBrain/luigi-tools/pull/71))

### New Features

- Add simple function to export dependency graphs (Adrien Berchet - [#74](https://github.com/BlueBrain/luigi-tools/pull/74))
- RerunMixin can be used with luigi.WrapperTask (Adrien Berchet - [#73](https://github.com/BlueBrain/luigi-tools/pull/73))

### CI Improvements

- Add job for min versions (Adrien Berchet - [#75](https://github.com/BlueBrain/luigi-tools/pull/75))

## [0.2.1](https://github.com/BlueBrain/luigi-tools/compare/0.2.0..0.2.1)

> 13 January 2023

### New Features

- Can pass a JSON schema to ListParameter (Adrien Berchet - [#69](https://github.com/BlueBrain/luigi-tools/pull/69))

## [0.2.0](https://github.com/BlueBrain/luigi-tools/compare/0.1.1..0.2.0)

> 12 January 2023

### New Features

- Can pass a JSON schema to DictParameter (Adrien Berchet - [#66](https://github.com/BlueBrain/luigi-tools/pull/66))

### Fixes

- Reload config from LUIGI_CONFIG_PATH after the template and luigi.cfg (Adrien Berchet - [#65](https://github.com/BlueBrain/luigi-tools/pull/65))

## [0.1.1](https://github.com/BlueBrain/luigi-tools/compare/0.1.0..0.1.1)

> 6 December 2022

### CI Improvements

- Add cache for APT, precommit and tox environments (Adrien Berchet - [#62](https://github.com/BlueBrain/luigi-tools/pull/62))
- Setup CodeQL (Adrien Berchet - [#59](https://github.com/BlueBrain/luigi-tools/pull/59))
- Setup Dependabot (Adrien Berchet - [#58](https://github.com/BlueBrain/luigi-tools/pull/58))

### Fixes

- Dataclass issue with optional dataclass attributes (Eleftherios Zisis - [#60](https://github.com/BlueBrain/luigi-tools/pull/60))

## [0.1.0](https://github.com/BlueBrain/luigi-tools/compare/0.0.19..0.1.0)

> 28 November 2022

### New Features

- Add support for Python 3.11 (Adrien Berchet - [#56](https://github.com/BlueBrain/luigi-tools/pull/56))
- Add DataclassParameter (Eleftherios Zisis - [#53](https://github.com/BlueBrain/luigi-tools/pull/53))

### Chores And Housekeeping

- Deprecate Python 3.6 (Adrien Berchet - [#54](https://github.com/BlueBrain/luigi-tools/pull/54))

### Documentation Changes

- Fix pydocstyle paths and fix docstrings accordingly (Adrien Berchet - [#50](https://github.com/BlueBrain/luigi-tools/pull/50))
- Fix requirements for doc generation (Adrien Berchet - [#42](https://github.com/BlueBrain/luigi-tools/pull/42))
- Fix changelog generation (Adrien Berchet - [#43](https://github.com/BlueBrain/luigi-tools/pull/43))

### CI Improvements

- Apply Copier template (Adrien Berchet - [#55](https://github.com/BlueBrain/luigi-tools/pull/55))
- Export pytest and coverage reports (Adrien Berchet - [#45](https://github.com/BlueBrain/luigi-tools/pull/45))
- Use commitlint to check PR titles (Adrien Berchet - [#40](https://github.com/BlueBrain/luigi-tools/pull/40))

## [0.0.19](https://github.com/BlueBrain/luigi-tools/compare/0.0.18..0.0.19)

> 29 June 2022

### New Features

- Support orphan nodes in dependency graphs (Adrien Berchet - [#37](https://github.com/BlueBrain/luigi-tools/pull/37))

### Chores And Housekeeping

- Remove deprecated features that were introduced in `luigi == 3.1.0` (Adrien Berchet - [#35](https://github.com/BlueBrain/luigi-tools/pull/35))

## [0.0.18](https://github.com/BlueBrain/luigi-tools/compare/0.0.17..0.0.18)

> 3 June 2022

### CI Improvements

- Move black, codespell, isort, pycodestyle and pydocstyle from tox to pre-commit (Adrien Berchet - [#27](https://github.com/BlueBrain/luigi-tools/pull/27))
- Setup pre-commit and commitlint (Adrien Berchet - [#26](https://github.com/BlueBrain/luigi-tools/pull/26))
- Add isort and codespell in lint and rework tox jobs (Adrien Berchet - [#25](https://github.com/BlueBrain/luigi-tools/pull/25))

### New Features

- Add warnings for config parameters that are not consumed by a task (#32) (Adrien Berchet - [7c054f7](https://github.com/BlueBrain/luigi-tools/commit/7c054f7a72377cf72b5fcfdd2bf24247508103fb))

## [0.0.17](https://github.com/BlueBrain/luigi-tools/compare/0.0.16..0.0.17)

> 17 March 2022

### New Features

- Improve the behavior of prefixes in targets and add custom str representations to them (Adrien Berchet - [#23](https://github.com/BlueBrain/luigi-tools/pull/23))

### Chores And Housekeeping

- Fix typo (Adrien Berchet - [#22](https://github.com/BlueBrain/luigi-tools/pull/22))

## [0.0.16](https://github.com/BlueBrain/luigi-tools/compare/0.0.15..0.0.16)

> 11 January 2022

- Can use a raw string as __prefix for OutputLocalTarget classes (Adrien Berchet - [#20](https://github.com/BlueBrain/luigi-tools/pull/20))
- Add deprecation warnings helpers for features moved to official luigi package (Adrien Berchet - [#19](https://github.com/BlueBrain/luigi-tools/pull/19))
- OptionalParameter can now have an iterable set as expected_type (Adrien Berchet - [#18](https://github.com/BlueBrain/luigi-tools/pull/18))

## [0.0.15](https://github.com/BlueBrain/luigi-tools/compare/0.0.14..0.0.15)

> 14 October 2021

- Add a PathParameter to easily handle file paths (Adrien Berchet - [#15](https://github.com/BlueBrain/luigi-tools/pull/15))
- Fix ExtParameter so it is also processed for default values (Adrien Berchet - [f080949](https://github.com/BlueBrain/luigi-tools/commit/f080949b736323078fb05a68f9e6b5aaa3877ca8))

## [0.0.14](https://github.com/BlueBrain/luigi-tools/compare/0.0.13..0.0.14)

> 7 October 2021

- Fix: __prefix attribute is no more mandatory for OutputLocalTarget children (Adrien Berchet - [#14](https://github.com/BlueBrain/luigi-tools/pull/14))

## [0.0.13](https://github.com/BlueBrain/luigi-tools/compare/0.0.12..0.0.13)

> 7 October 2021

- Add py39 in CI (Adrien Berchet - [#12](https://github.com/BlueBrain/luigi-tools/pull/12))
- Add an example in README for RemoveCorruptedOutput (Anil Tuncel - [#11](https://github.com/BlueBrain/luigi-tools/pull/11))
- Improve OutputLocalTarget to make the creation of directory trees easier (Adrien Berchet - [#10](https://github.com/BlueBrain/luigi-tools/pull/10))

## [0.0.12](https://github.com/BlueBrain/luigi-tools/compare/0.0.11..0.0.12)

> 12 August 2021

- Improve OptionalParameter which is now a true mixin (Adrien Berchet - [#7](https://github.com/BlueBrain/luigi-tools/pull/7))
- Fix task_value() with @copy_params and improve tests (Adrien Berchet - [#8](https://github.com/BlueBrain/luigi-tools/pull/8))
- Fix license in docstrings (Adrien Berchet - [bcce989](https://github.com/BlueBrain/luigi-tools/commit/bcce989d99a0221c60de787b9061ee03b7fcebe5))

## [0.0.11](https://github.com/BlueBrain/luigi-tools/compare/0.0.10..0.0.11)

> 21 July 2021

- Remove devpi and fix RTD URL (Adrien Berchet - [#6](https://github.com/BlueBrain/luigi-tools/pull/6))
- Setup ReadTheDocs (Adrien Berchet - [#5](https://github.com/BlueBrain/luigi-tools/pull/5))
- Update README.md (alex4200 - [#4](https://github.com/BlueBrain/luigi-tools/pull/4))
- Validate OSS checklist (Adrien Berchet - [#3](https://github.com/BlueBrain/luigi-tools/pull/3))
- Change license and use version scm (Adrien Berchet - [#1](https://github.com/BlueBrain/luigi-tools/pull/1))
- Migrate from Gerrit to GitHub (Adrien Berchet - [54d7fff](https://github.com/BlueBrain/luigi-tools/commit/54d7fff98d868e2f6587bd7a62b909444c6e250e))
- Remove version file (Adrien Berchet - [bb6776a](https://github.com/BlueBrain/luigi-tools/commit/bb6776ad9f5660d2365952c2a5bbb851b7dc4737))
- Update publish-sdist.yml (Adrien Berchet - [a280a02](https://github.com/BlueBrain/luigi-tools/commit/a280a022d96cd4530c3b580d992619df8effc562))
- Fix long description in setup.py (Adrien Berchet - [a923e6a](https://github.com/BlueBrain/luigi-tools/commit/a923e6acb7cf3b9afa63185ac7486669a1173034))

## [0.0.10](https://github.com/BlueBrain/luigi-tools/compare/0.0.9..0.0.10)

> 21 April 2021

- Fix empty string case in OptionalParameter (Adrien Berchet - [ef45da1](https://github.com/BlueBrain/luigi-tools/commit/ef45da101951247347f3fb91c4ac1f605b85f25c))

## [0.0.9](https://github.com/BlueBrain/luigi-tools/compare/0.0.8..0.0.9)

> 19 April 2021

- Add OptionalBoolParameter (Adrien Berchet - [e5278c4](https://github.com/BlueBrain/luigi-tools/commit/e5278c4ed21f53aed1f606ecf0cfe809247598d3))

## [0.0.8](https://github.com/BlueBrain/luigi-tools/compare/0.0.7..0.0.8)

> 15 March 2021

- Add set_luigi_config() context manager to use temporary luigi config (Adrien Berchet - [dbdcfc8](https://github.com/BlueBrain/luigi-tools/commit/dbdcfc8b4ba566bbb18264c7e199d85ddd828360))

## [0.0.7](https://github.com/BlueBrain/luigi-tools/compare/0.0.6..0.0.7)

> 22 February 2021

- Improve test readability (Adrien Berchet - [587e21f](https://github.com/BlueBrain/luigi-tools/commit/587e21fd9e63490c01b11d587dbf4042dbd545f7))
- Add dependency graph rendering with GraphViz (Adrien Berchet - [69ab397](https://github.com/BlueBrain/luigi-tools/commit/69ab397eb9852d6c2b49b83ad4df6f66ee598943))
- Add a function to register config templates (Adrien Berchet - [d3a1e31](https://github.com/BlueBrain/luigi-tools/commit/d3a1e317b1a4799e5e03a310e9e35d4cf77c7bc8))
- Add OptionalStrParameter (Adrien Berchet - [43a0df2](https://github.com/BlueBrain/luigi-tools/commit/43a0df20e00fc00da022a7da33844a32706e35d1))

## [0.0.6](https://github.com/BlueBrain/luigi-tools/compare/0.0.5..0.0.6)

> 4 February 2021

- Merge "Check name of global parameters before their values in GlobalParamMixin.__setattr__" (Adrien Berchet - [e90b5f5](https://github.com/BlueBrain/luigi-tools/commit/e90b5f5150a59258e3b795c83e36bfc7e9f4303e))
- add RemoveCorruptedOutputMixin (Anil Tuncel - [7c7a7fc](https://github.com/BlueBrain/luigi-tools/commit/7c7a7fc156a2f05ac1113c9cc9a2e9c528a6cde2))
- Check name of global parameters before their values in GlobalParamMixin.__setattr__ (Adrien Berchet - [4308ea6](https://github.com/BlueBrain/luigi-tools/commit/4308ea646c64744d791b649727d6e9e409baf2dd))

## [0.0.5](https://github.com/BlueBrain/luigi-tools/compare/0.0.4..0.0.5)

> 4 January 2021

- Improve luigi config files in tests (Adrien Berchet - [8d8220f](https://github.com/BlueBrain/luigi-tools/commit/8d8220f977a9c7db904c650a327e9a6af3a8c16b))
- Set default value of OutputLocalTarget.__init__.create_parent() parameter to True (Adrien Berchet - [7165d0f](https://github.com/BlueBrain/luigi-tools/commit/7165d0fc03bfe66a30d4608090398417c8421e57))
- Update changelog (Adrien Berchet - [56dc7a8](https://github.com/BlueBrain/luigi-tools/commit/56dc7a87088c9c9f598f63dd2f598b2e72e93f81))

## [0.0.4](https://github.com/BlueBrain/luigi-tools/compare/0.0.3..0.0.4)

> 4 January 2021

- Add new optional parameters and improve OutputLocalTarget (Adrien Berchet - [b586284](https://github.com/BlueBrain/luigi-tools/commit/b5862846ee8a106815ca6358fc3648e26a0562e1))

## [0.0.3](https://github.com/BlueBrain/luigi-tools/compare/0.0.2..0.0.3)

> 30 November 2020

- module name refactoring to use singular and be consistent with luigi (genrich - [50cc72a](https://github.com/BlueBrain/luigi-tools/commit/50cc72a728592a34b1dabba6cf9b6a2cd2c4fed9))
- release (genrich - [94a88e2](https://github.com/BlueBrain/luigi-tools/commit/94a88e2576f7211f34554133bec47d583f716462))

## [0.0.2](https://github.com/BlueBrain/luigi-tools/compare/0.0.1..0.0.2)

> 30 November 2020

- Fix GlobalParamMixin for serialized parameters (Adrien Berchet - [d11bb77](https://github.com/BlueBrain/luigi-tools/commit/d11bb776ec179783e2e2af21e9599caeec83318b))
- Improve doc, especially for copy_params and OutputLocalTarget (Adrien Berchet - [31171f2](https://github.com/BlueBrain/luigi-tools/commit/31171f2c16db7dda980a5d3f3f0bd5a09524853e))
- Rename ParamLink to ParamRef which is less ambiguous (Adrien Berchet - [f0c4d77](https://github.com/BlueBrain/luigi-tools/commit/f0c4d77d10b7608c48eb5dc0307ca1047eb67791))
- Fix OptionalParameter.normalize (Adrien Berchet - [ab657d5](https://github.com/BlueBrain/luigi-tools/commit/ab657d58e4cda3659b684c2ab1fcef5c9dbd0d8a))
- Update changelog before release (Adrien Berchet - [ac30e98](https://github.com/BlueBrain/luigi-tools/commit/ac30e98eeecd9dd6a27fa6bb5d72a41be1a8e8ba))

## 0.0.1

> 27 November 2020

- First commit (Adrien Berchet - [65eb608](https://github.com/BlueBrain/luigi-tools/commit/65eb608d9244633816abf7a4c996836bdda5bbfc))
- migrate sphinx docs from autoapi to autodoc (genrich - [49f08e2](https://github.com/BlueBrain/luigi-tools/commit/49f08e2f23556bafe5f204f3f059622074595bd6))
- Transform the RerunnableTask into a mixin called RerunMixin (Adrien Berchet - [ba5173b](https://github.com/BlueBrain/luigi-tools/commit/ba5173b6720cd55a527bccaddd793e5228711e90))
- Initial empty repository (Dries Verachtert - [69d109b](https://github.com/BlueBrain/luigi-tools/commit/69d109bd749dedc8f5533a3649e29825c8e21d78))
