# Apex Malhar Documentation

Repository for Malhar docs available on http://apex.apache.org/docs.html

Documentation is written in [Markdown](https://guides.github.com/features/mastering-markdown/) format and statically generated into HTML using [MkDocs](http://www.mkdocs.org/).  All documentation is located in the [docs](docs) directory, and [mkdocs.yml](mkdocs.yml) file describes the navigation structure of the published documentation.

## Authoring

New pages can be added under [docs](docs) or related sub-category, and a reference to the new page must be added to the [mkdocs.yml](mkdocs.yml) file to make it availabe in the navigation.  Embedded images are typically added to images folder at the same level as the new page.

When creating or editing pages, it can be useful to see the live results, and how the documents will appear when published.  Live preview feature is available by running the following command at the root of the repository:

```bash
mkdocs serve
```

For additional details see [writing your docs](http://www.mkdocs.org/user-guide/writing-your-docs/) guide.

## Site Configuration

Guides on applying site-wide [configuration](http://www.mkdocs.org/user-guide/configuration/) and [themeing](http://www.mkdocs.org/user-guide/styling-your-docs/) are available on the MkDocs site.

## Deployment

Deployment is done in two steps.  First all documentation is statically generatd into HTML files and then it is deployed to the apex website.  For more details on how conversion to HTML works see [MkDocs documentation](http://www.mkdocs.org/).

1.  Go to release branch of the repository and execute the following command to build the docs:

```bash
# set project version
APEX_MALHAR_VERSION=3.4

# build docs under site foolder
mkdocs build --clean

# copy docs from site into target folder on apex-site
cd ../incubator-apex-site
git checkout asf-site
cp -r ../incubator-apex-malhar/site docs/malhar-${APEX_MALHAR_VERSION}
git add -A
git commit -m "Adding apex-${APEX_MALHAR_VERSION} documentation"
git push
```

2.  Go to [apex-site repository](https://github.com/apache/incubator-apex-site#contributing) and add the new link to the [docs.md](https://github.com/apache/incubator-apex-site/blob/master/src/md/docs.md) and follow committer steps to commit and push these changes, and deploy the site.
