from test.integration.base import DBTIntegrationTest, use_profile, normalize
import yaml
import shutil
import os


class TestBuildBase(DBTIntegrationTest):
    @property
    def schema(self):
        return "build_test_069"

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["snapshots"],
            "seed-paths": ["seeds"],
            "seeds": {
                "quote_columns": False,
            },
        }

    def build(self, expect_pass=True, extra_args=None, **kwargs):
        args = ["build"]
        if kwargs:
            args.extend(("--args", yaml.safe_dump(kwargs)))
        if extra_args:
            args.extend(extra_args)

        return self.run_dbt(args, expect_pass=expect_pass)


class TestPassingBuild(TestBuildBase):
    @property
    def models(self):
        return "models"

    @use_profile("postgres")
    def test__postgres_build_happy_path(self):
        self.build()


class TestFailingBuild(TestBuildBase):
    @property
    def models(self):
        return "models-failing"

    @use_profile("postgres")
    def test__postgres_build_happy_path(self):
        results = self.build(expect_pass=False)
        self.assertEqual(len(results), 13)
        actual = [r.status for r in results]
        expected = ['error']*1 + ['skipped']*5 + ['pass']*2 + ['success']*5
        self.assertEqual(sorted(actual), sorted(expected))


class TestFailingTestsBuild(TestBuildBase):
    @property
    def models(self):
        return "tests-failing"

    @use_profile("postgres")
    def test__postgres_failing_test_skips_downstream(self):
        results = self.build(expect_pass=False)
        self.assertEqual(len(results), 13)
        actual = [str(r.status) for r in results]
        expected = ['fail'] + ['skipped']*6 + ['pass']*2 + ['success']*4
        self.assertEqual(sorted(actual), sorted(expected))


class TestCircularRelationshipTestsBuild(TestBuildBase):
    @property
    def models(self):
        return "models-circular-relationship"

    @use_profile("postgres")
    def test__postgres_circular_relationship_test_success(self):
        """ Ensure that tests that refer to each other's model don't create
        a circular dependency. """
        results = self.build()
        actual = [r.status for r in results]
        expected = ['success']*7 + ['pass']*2
        self.assertEqual(sorted(actual), sorted(expected))


class TestSimpleBlockingTest(TestBuildBase):
    @property
    def models(self):
        return "models-simple-blocking"
        
    @property
    def project_config(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["does-not-exist"],
            "seed-paths": ["does-not-exist"],
        }

    @use_profile("postgres")
    def test__postgres_simple_blocking_test(self):
        """ Ensure that a failed test on model_a always blocks model_b """
        results = self.build(expect_pass=False)
        actual = [r.status for r in results]
        expected = ['success', 'fail', 'skipped']
        self.assertEqual(sorted(actual), sorted(expected))


class TestInterdependentModels(TestBuildBase):

    @property
    def project_config(self):
        return {
            "config-version": 2,
            "snapshot-paths": ["snapshots-none"],
            "seeds": {
                "quote_columns": False,
            },
        }

    @property
    def models(self):
        return "models-interdependent"

    def tearDown(self):
        if os.path.exists(normalize('models-interdependent/model_b.sql')):
            os.remove(normalize('models-interdependent/model_b.sql'))


    @use_profile("postgres")
    def test__postgres_interdependent_models(self):
        # check that basic build works
        shutil.copyfile('test-files/model_b.sql', 'models-interdependent/model_b.sql')
        results = self.build()
        self.assertEqual(len(results), 16)

        # return null from model_b
        shutil.copyfile('test-files/model_b_null.sql', 'models-interdependent/model_b.sql')
        results = self.build(expect_pass=False)
        self.assertEqual(len(results), 16)
        actual = [str(r.status) for r in results]
        expected = ['error']*4 + ['skipped']*7 + ['pass']*2 + ['success']*3
        self.assertEqual(sorted(actual), sorted(expected))

