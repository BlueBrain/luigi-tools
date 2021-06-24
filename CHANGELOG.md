# Changelog

## luigi-tools-v0.0.11

> 29 June 2021

- First commit (Adrien Berchet - [65eb608](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/65eb608d9244633816abf7a4c996836bdda5bbfc))
- Improve test readability (Adrien Berchet - [587e21f](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/587e21fd9e63490c01b11d587dbf4042dbd545f7))
- Add dependency graph rendering with GraphViz (Adrien Berchet - [69ab397](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/69ab397eb9852d6c2b49b83ad4df6f66ee598943))
- module name refactoring to use singular and be consistent with luigi (genrich - [50cc72a](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/50cc72a728592a34b1dabba6cf9b6a2cd2c4fed9))
- Improve luigi config files in tests (Adrien Berchet - [8d8220f](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/8d8220f977a9c7db904c650a327e9a6af3a8c16b))
- Add set_luigi_config() context manager to use temporary luigi config (Adrien Berchet - [dbdcfc8](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/dbdcfc8b4ba566bbb18264c7e199d85ddd828360))
- Add a function to register config templates (Adrien Berchet - [d3a1e31](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/d3a1e317b1a4799e5e03a310e9e35d4cf77c7bc8))
- Add auto-release CI job (Adrien Berchet - [f9be313](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/f9be313f2ad53879f679010c114d462588f234ba))
- Add new optional parameters and improve OutputLocalTarget (Adrien Berchet - [b586284](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/b5862846ee8a106815ca6358fc3648e26a0562e1))
- Fix GlobalParamMixin for serialized parameters (Adrien Berchet - [d11bb77](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/d11bb776ec179783e2e2af21e9599caeec83318b))
- Improve doc, especially for copy_params and OutputLocalTarget (Adrien Berchet - [31171f2](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/31171f2c16db7dda980a5d3f3f0bd5a09524853e))
- migrate sphinx docs from autoapi to autodoc (genrich - [49f08e2](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/49f08e2f23556bafe5f204f3f059622074595bd6))
- add RemoveCorruptedOutputMixin (Anil Tuncel - [7c7a7fc](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/7c7a7fc156a2f05ac1113c9cc9a2e9c528a6cde2))
- Check name of global parameters before their values in GlobalParamMixin.__setattr__ (Adrien Berchet - [4308ea6](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/4308ea646c64744d791b649727d6e9e409baf2dd))
- Migrate the CI from Jenkins to GitLab (Adrien Berchet - [8b6c819](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/8b6c8199f17e695593bc35d90d75331410da0fdf))
- Rename ParamLink to ParamRef which is less ambiguous (Adrien Berchet - [f0c4d77](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/f0c4d77d10b7608c48eb5dc0307ca1047eb67791))
- Fix empty string case in OptionalParameter (Adrien Berchet - [ef45da1](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/ef45da101951247347f3fb91c4ac1f605b85f25c))
- Add OptionalBoolParameter (Adrien Berchet - [e5278c4](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/e5278c4ed21f53aed1f606ecf0cfe809247598d3))
- Add OptionalStrParameter (Adrien Berchet - [43a0df2](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/43a0df20e00fc00da022a7da33844a32706e35d1))
- Fix OptionalParameter.normalize (Adrien Berchet - [ab657d5](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/ab657d58e4cda3659b684c2ab1fcef5c9dbd0d8a))
- Update changelog before release (Adrien Berchet - [ac30e98](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/ac30e98eeecd9dd6a27fa6bb5d72a41be1a8e8ba))
- Fix source URL (Adrien Berchet - [a8245b2](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/a8245b211ec7b89baca47a9b3653f5d0e652143b))
- Merge "Check name of global parameters before their values in GlobalParamMixin.__setattr__" (Adrien Berchet - [e90b5f5](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/e90b5f5150a59258e3b795c83e36bfc7e9f4303e))
- Set default value of OutputLocalTarget.__init__.create_parent() parameter to True (Adrien Berchet - [7165d0f](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/7165d0fc03bfe66a30d4608090398417c8421e57))
- Update changelog (Adrien Berchet - [56dc7a8](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/56dc7a87088c9c9f598f63dd2f598b2e72e93f81))
- Transform the RerunnableTask into a mixin called RerunMixin (Adrien Berchet - [ba5173b](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/ba5173b6720cd55a527bccaddd793e5228711e90))
- release (genrich - [94a88e2](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/94a88e2576f7211f34554133bec47d583f716462))
- Initial empty repository (Dries Verachtert - [69d109b](https://bbpgitlab.epfl.ch/neuromath/luigi-tools/commit/69d109bd749dedc8f5533a3649e29825c8e21d78))
