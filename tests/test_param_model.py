import pytest
from pydantic import ValidationError

from dist_s1_enumerator.param_models import LookbackStrategyParams


class TestLookbackStrategyParams:
    """Test cases for lookback_strategy_params validation."""

    def test_multi_window_mismatched_tuple_lengths(self) -> None:
        """Test validation error when delta_lookback_days and max_pre_imgs_per_burst have different lengths."""
        with pytest.raises(ValidationError, match='must have the same length'):
            LookbackStrategyParams(
                lookback_strategy='multi_window',
                max_pre_imgs_per_burst=(5, 4, 3),  # Length 3
                delta_lookback_days=(365, 730),  # Length 2 - should fail
                min_pre_imgs_per_burst=1,
                delta_window_days=30,
            )

    def test_multi_window_min_greater_than_max_tuple(self) -> None:
        """Test validation error when min_pre_imgs_per_burst > max_pre_imgs_per_burst (tuple case)."""
        with pytest.raises(ValidationError, match='must be greater than min_pre_imgs_per_burst'):
            LookbackStrategyParams(
                lookback_strategy='multi_window',
                max_pre_imgs_per_burst=(5, 4, 3),
                delta_lookback_days=(365, 730, 1095),
                min_pre_imgs_per_burst=6,  # Greater than all values in max_pre_imgs_per_burst
                delta_window_days=30,
            )

    def test_multi_window_min_greater_than_max_int(self) -> None:
        """Test validation error when min_pre_imgs_per_burst > max_pre_imgs_per_burst (int case)."""
        with pytest.raises(ValidationError, match='must be greater than min_pre_imgs_per_burst'):
            LookbackStrategyParams(
                lookback_strategy='multi_window',
                max_pre_imgs_per_burst=5,
                delta_lookback_days=365,
                min_pre_imgs_per_burst=6,  # Greater than max_pre_imgs_per_burst
                delta_window_days=30,
            )

    def test_immediate_lookback_delta_days_tuple_error(self) -> None:
        """Test validation error when delta_lookback_days is not 0 for immediate_lookback."""
        with pytest.raises(ValidationError, match='delta_lookback_days must be 0 for immediate lookback strategy'):
            LookbackStrategyParams(
                lookback_strategy='immediate_lookback',
                max_pre_imgs_per_burst=5,
                delta_lookback_days=365,  # Should be 0 for immediate_lookback
                min_pre_imgs_per_burst=1,
                delta_window_days=30,
            )

    def test_immediate_lookback_max_pre_imgs_tuple_error(self) -> None:
        """Test validation error when max_pre_imgs_per_burst is tuple for immediate_lookback."""
        with pytest.raises(ValidationError, match='must be a single integer for immediate lookback strategy'):
            LookbackStrategyParams(
                lookback_strategy='immediate_lookback',
                max_pre_imgs_per_burst=(5, 4, 3),  # Should be int for immediate_lookback
                delta_lookback_days=0,
                min_pre_imgs_per_burst=1,
                delta_window_days=30,
            )

    def test_immediate_lookback_min_greater_than_max(self) -> None:
        """Test validation error when min_pre_imgs_per_burst > max_pre_imgs_per_burst for immediate_lookback."""
        with pytest.raises(ValidationError, match='must be greater than min_pre_imgs_per_burst'):
            LookbackStrategyParams(
                lookback_strategy='immediate_lookback',
                max_pre_imgs_per_burst=5,
                delta_lookback_days=0,
                min_pre_imgs_per_burst=6,  # Greater than max_pre_imgs_per_burst
                delta_window_days=30,
            )

    def test_multi_window_equivalent_configurations(self) -> None:
        """Test that different ways of specifying multi_window configuration are equivalent."""
        # Configuration 1: All as integers
        config1 = LookbackStrategyParams(
            lookback_strategy='multi_window',
            max_pre_imgs_per_burst=5,
            delta_lookback_days=365,
            min_pre_imgs_per_burst=1,
            delta_window_days=30,
        )

        # Configuration 2: Explicit tuples with calculated values
        config2 = LookbackStrategyParams(
            lookback_strategy='multi_window',
            max_pre_imgs_per_burst=(5, 5, 5),
            delta_lookback_days=(365, 730, 1095),  # 1*365, 2*365, 3*365
            min_pre_imgs_per_burst=1,
            delta_window_days=30,
        )

        # Configuration 3: Mixed - tuple for max, int for delta (should expand to tuple)
        config3 = LookbackStrategyParams(
            lookback_strategy='multi_window',
            max_pre_imgs_per_burst=(5, 5, 5),
            delta_lookback_days=365,
            min_pre_imgs_per_burst=1,
            delta_window_days=30,
        )

        expected_delta_lookback_days = (365, 730, 1095)
        # Verify that config1 gets normalized to expected tuple formats
        assert config1.max_pre_imgs_per_burst == (5, 5, 5)
        assert config1.delta_lookback_days == expected_delta_lookback_days

        # Verify that config2 maintains its explicit values
        assert config2.max_pre_imgs_per_burst == (5, 5, 5)
        assert config2.delta_lookback_days == expected_delta_lookback_days

        # Verify that config3 gets normalized properly
        assert config3.max_pre_imgs_per_burst == (5, 5, 5)
        assert config3.delta_lookback_days == expected_delta_lookback_days

        # All should have the same basic structure (same max_pre_imgs_per_burst)
        assert config1.max_pre_imgs_per_burst == config3.max_pre_imgs_per_burst
        assert (
            len(config1.max_pre_imgs_per_burst)
            == len(config2.max_pre_imgs_per_burst)
            == len(config3.max_pre_imgs_per_burst)
        )

    def test_multi_window_list_to_tuple_conversion(self) -> None:
        """Test that lists are properly converted to tuples for multi_window strategy."""
        config = LookbackStrategyParams(
            lookback_strategy='multi_window',
            max_pre_imgs_per_burst=[5, 4, 3],  # List input
            delta_lookback_days=[365, 730, 1095],  # List input
            min_pre_imgs_per_burst=1,
            delta_window_days=30,
        )

        # Should be converted to tuples
        assert config.max_pre_imgs_per_burst == (5, 4, 3)
        assert config.delta_lookback_days == (365, 730, 1095)
        assert isinstance(config.max_pre_imgs_per_burst, tuple)
        assert isinstance(config.delta_lookback_days, tuple)

    def test_delta_window_days_validation(self) -> None:
        """Test validation of delta_window_days > 365."""
        with pytest.raises(ValidationError, match='delta_window_days must be less than 365 days'):
            LookbackStrategyParams(
                lookback_strategy='multi_window',
                max_pre_imgs_per_burst=5,
                delta_lookback_days=365,
                min_pre_imgs_per_burst=1,
                delta_window_days=400,  # > 365, should fail
            )

    def test_invalid_lookback_strategy(self) -> None:
        """Test validation error for invalid lookback_strategy."""
        with pytest.raises(ValidationError, match='lookback_strategy must be one of'):
            LookbackStrategyParams(
                lookback_strategy='invalid_strategy',
                max_pre_imgs_per_burst=5,
                delta_lookback_days=365,
                min_pre_imgs_per_burst=1,
                delta_window_days=30,
            )

    def test_valid_immediate_lookback_configuration(self) -> None:
        """Test a valid immediate_lookback configuration."""
        config = LookbackStrategyParams(
            lookback_strategy='immediate_lookback',
            max_pre_imgs_per_burst=5,
            delta_lookback_days=0,
            min_pre_imgs_per_burst=1,
            delta_window_days=30,
        )

        assert config.lookback_strategy == 'immediate_lookback'
        assert config.max_pre_imgs_per_burst == 5
        assert config.delta_lookback_days == 0
        assert config.min_pre_imgs_per_burst == 1
        assert config.delta_window_days == 30

    def test_valid_multi_window_configuration(self) -> None:
        """Test a valid multi_window configuration."""
        config = LookbackStrategyParams(
            lookback_strategy='multi_window',
            max_pre_imgs_per_burst=(5, 4, 3),
            delta_lookback_days=(365, 730, 1095),
            min_pre_imgs_per_burst=2,
            delta_window_days=30,
        )

        assert config.lookback_strategy == 'multi_window'
        assert config.max_pre_imgs_per_burst == (5, 4, 3)
        assert config.delta_lookback_days == (365, 730, 1095)
        assert config.min_pre_imgs_per_burst == 2
        assert config.delta_window_days == 30
